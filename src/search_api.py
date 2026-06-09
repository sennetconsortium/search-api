import sys

if "search-adaptor/src" not in sys.path:
    sys.path.append("search-adaptor/src")

from app import SearchAPI
from opensearch_helper_functions import unauthorized_error

from senotype import senotypes_blueprint


class SenNetSearchAPI(SearchAPI):
    INDICES = {}
    SENOTYPE_EDIT_GROUP_UUID = None

    def __init__(
        self,
        config,
        translator_module,
        blueprint=None,
        ubkg_instance=None,
        progress_interface=None,
    ):
        super().__init__(config, translator_module, blueprint, ubkg_instance, progress_interface)
        self.app.config.update(config)
        self.app.register_blueprint(senotypes_blueprint)
        self.app.ubkg = ubkg_instance

    def get_target_index(self, request, index_without_prefix):
        target_index = super().get_target_index(request, index_without_prefix)

        if index_without_prefix in ["senotypes", "senotypes-test"]:
            target_index = self.INDICES["indices"][index_without_prefix]["public"]
            if not request.authorization or not self.SENOTYPE_EDIT_GROUP_UUID:
                return target_index

            user_info = self.auth_helper_instance.getUserInfoUsingRequest(request, True)
            if not isinstance(user_info, dict):
                # this shouldn't occur since super().get_target_index() should have already
                # verified the token
                unauthorized_error(
                    "The globus token in the HTTP 'Authorization: Bearer <globus-token>' "
                    "header is either invalid or expired."
                )
                return

            # check if user belongs to the senotype edit group, if so, direct the call to
            # the private senotypes index
            if self.SENOTYPE_EDIT_GROUP_UUID in user_info.get("hmgroupids", []):
                target_index = self.INDICES["indices"][index_without_prefix]["private"]

        return target_index
