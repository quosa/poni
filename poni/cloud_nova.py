"""
Cloud-provider implementation: OpenStack Nova using python-novaclient

"""
import os
import copy
import time
import logging
from . import errors
from . import cloudbase

NOVA_ID = "nova"

try:
    from novaclient.v1_1 import client as nova_client
    from novaclient import exceptions as nova_exceptions
    # Debug logging
    #import novaclient.client
    #ch = logging.StreamHandler()
    #novaclient.client._logger.setLevel(logging.DEBUG)
    #novaclient.client._logger.addHandler(ch)
except ImportError:
    nova_client = None
    nova_exceptions = None


def convert_nova_errors(method):
    """Convert remote nova errors to errors.CloudError"""
    def wrapper(self, *args, **kw):
        try:
            return method(self, *args, **kw)
        except nova_exceptions.ClientException as error:
            raise errors.CloudError("{0.__class__.__name__}: {0}".format(error))

    wrapper.__doc__ = method.__doc__
    wrapper.__name__ = method.__name__

    return wrapper


class NovaProvider(cloudbase.Provider):
    @classmethod
    def amend_cloud_props(cls, prop):
        """Apply default values from environment"""
        prop = prop.copy()
        defaults = (
            ("username", "PONI_NOVA_USERNAME", "OS_USERNAME"),
            ("password", "PONI_NOVA_PASSWORD", "OS_PASSWORD"),
            ("tenant", "PONI_NOVA_TENANT", "OS_TENANT_NAME"),
            ("auth_url", "PONI_NOVA_AUTH_URL", "OS_AUTH_URL"),
            ("region", "PONI_NOVA_REGION", "OS_REGION_NAME"),
            )
        for key, env_var, env_nova in defaults:
            prop.setdefault(key, os.environ.get(env_var, os.environ.get(env_nova)))

        return prop

    @classmethod
    def get_provider_key(cls, cloud_prop):
        cloud_prop = cls.amend_cloud_props(cloud_prop)
        required = ["username", "password", "tenant", "auth_url", "region"]
        missing = []
        for prop in required:
            value = cloud_prop.get(prop)
            if not value:
                missing.append(prop)

        if missing:
            raise errors.CloudError(
                "required nova cloud properties missing: %s" % (", ".join(missing)))

        return (cloud_prop[p] for p in ["username", "tenant", "auth_url", "region"])

    def __init__(self, cloud_prop):
        assert nova_client, "python-novaclient is not installed, cannot access Nova"
        cloud_prop = self.amend_cloud_props(cloud_prop)

        cloudbase.Provider.__init__(self, NOVA_ID, cloud_prop)
        self.log = logging.getLogger(NOVA_ID)
        self.username = cloud_prop["username"]
        self.password = cloud_prop["password"]
        self.auth_url = cloud_prop["auth_url"]
        self.tenant = cloud_prop["tenant"]
        self.region = cloud_prop["region"]
        self._conn = None

    def _get_conn(self):
        if self._conn:
            return self._conn

        self._conn = nova_client.Client(self.username, self.password, self.tenant,
                                        self.auth_url, region_name=self.region)
        return self._conn

    @convert_nova_errors
    def init_instance(self, cloud_prop):
        conn = self._get_conn()
        try:
            vm_name = cloud_prop["vm_name"]
        except KeyError:
            raise errors.CloudError(
                "cloud 'vm_name' property required by Nova not defined")

        try:
            image_id = cloud_prop["image"]
        except KeyError:
            raise errors.CloudError(
                "cloud 'image' property required by Nova not defined")

        try:
            key_name = cloud_prop["key_pair"]
        except KeyError:
            raise errors.CloudError("'key_pair' cloud property not set")

        try:
            flavor = cloud_prop["flavor"]
        except KeyError:
            raise errors.CloudError(
                "cloud 'flavor' property required by Nova not defined")

        if isinstance(flavor, basestring):
            # resolve flavor name to a flavor integer id
            all_flavors = conn.flavors.list()
            flavor_hits = [f for f in all_flavors
                           if f.name == flavor]
            if not flavor_hits:
                raise errors.CloudError(
                    "Nova image 'flavor' named %r not found, available: %s"
                    % (", ".join(f.name for f in all_flavors)))

            flavor = flavor_hits[0].id


        security_groups = cloud_prop.get("security_groups")
        if security_groups and isinstance(security_groups, (basestring, unicode)):
            security_groups = [security_groups]

        instances = self._get_instances([cloud_prop])
        if instances:
            # this one already exists
            instance = instances[0]
        else:
            # VM does not exist and needs to be created
            instance = conn.servers.create(vm_name, image=image_id, flavor=flavor,
                                           security_groups=security_groups, key_name=key_name)

        out_prop = copy.deepcopy(cloud_prop)
        out_prop["instance"] = instance.id

        return dict(cloud=out_prop)

    @convert_nova_errors
    def assign_ip(self, props):
        raise NotImplementedError("Nova IP assignment not implemented")

    def _get_instances(self, props):
        conn = self._get_conn()
        server_ids = set(p.get("instance") for p in props)
        server_names = set(p["vm_name"] for p in props)
        return [server for server in conn.servers.list()
                if (server.id in server_ids) or (server.name in server_names)]

    @convert_nova_errors
    def get_instance_status(self, prop):
        conn = self._get_conn()
        try:
            server = conn.servers.get(prop["instance"])
            return server.status
        except nova_exceptions.NotFound:
            return None

    @convert_nova_errors
    def terminate_instances(self, props):
        for server in self._get_instances(props):
            server.delete()

    @convert_nova_errors
    def wait_instances(self, props, wait_state="running"):
        assert wait_state == "running", "Nova only supports waiting for 'running' state"
        pending = self._get_instances(props)
        output = {}
        while pending:
            for server in pending[:]:
                server.get() # refresh properties
                if server.status == "ACTIVE":
                    pending.remove(server)
                    if wait_state:
                        self.log.debug("%s (id=%s) entered state: %s", server.name,
                                       server.id, "running")
                    # NOTE: HP Cloud returns all addresses as "private"
                    # {u'private': [u'10.4.90.196', u'15.185.113.29']}
                    # See: https://answers.launchpad.net/nova/+question/185110
                    # ...and there is no DNS name available.
                    private_ip = server.networks["private"][0]
                    output[server.id] = dict(
                        host=private_ip,
                        private=dict(ip=private_ip, dns=private_ip)
                        )
                    if len(server.networks) >= 2:
                        # another IP address is available, assume it is the public one
                        public_ip = server.networks["private"][1]
                        output[server.id]["public"] = dict(ip=public_ip, dns=public_ip)

            if pending:
                self.log.info("[%s/%s] instances %r, waiting...",
                              len(output), len(output) + len(pending),
                              wait_state)
                time.sleep(5)

        return output
