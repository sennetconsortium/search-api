import abc
import logging
import sys
import threading
from typing import cast

from flask import request
from translator.translator_interface import TranslatorInterface

if "search-adaptor/src" not in sys.path:
    sys.path.append("search-adaptor/src")

from app import SearchAPI
from opensearch_helper_functions import internal_server_error, unauthorized_error

from senotype import senotypes_blueprint


class SenNetTranslatorInterface(TranslatorInterface):
    @classmethod
    def __subclasshook__(cls, subclass):
        # First, make sure it satisfies the base TranslatorInterface contract
        is_translator = super().__subclasshook__(subclass)
        if is_translator is NotImplemented:
            is_translator = False

        return (
            is_translator
            and hasattr(subclass, "translate_all")
            and callable(subclass.translate_all)
        )

    @abc.abstractmethod
    def translate_all(self):
        """Live reindex of all docs"""
        raise NotImplementedError


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
        self.logger = logging.getLogger()
        self.app.config.update(config)
        self.app.register_blueprint(senotypes_blueprint)
        self.app.ubkg = ubkg_instance

        @self.app.route("/reindex-all", methods=["PUT"])
        def __reindex_all():
            return self.reindex_all()

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

    def reindex_all(self):
        # The token needs to belong to the Data Admin group
        # to be able to trigger a live reindex for all documents
        token = self.get_user_token(request.headers, admin_access_required=True)
        saved_request = request.headers

        self.logger.debug(saved_request)

        try:
            translator = cast(SenNetTranslatorInterface, self.init_translator(token))
            threading.Thread(target=translator.translate_all, args=[]).start()

            self.logger.info("Started live reindex all")
        except Exception as e:
            self.logger.exception(e)
            internal_server_error(e)

        return "Request of live reindex all documents accepted", 202
