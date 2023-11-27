import logging
from typing import List, Set, Union

from nuvla.notifs.log import get_logger
from nuvla.notifs.matching.base import TaggedResourceSubsCfgMatcher
from nuvla.notifs.models.event import Event
from nuvla.notifs.models.resource import collection_all_owners
from nuvla.notifs.models.subscription import SubscriptionCfg
from nuvla.notifs.notification import BlackboxEventNotification, \
    AppPublishedDeploymentsUpdateNotification, \
    AppPublishedAppsBouquetUpdateNotification, \
    AppAppBqPublishedDeploymentGroupUpdateNotification
from nuvla.notifs.nuvla_api import init_nuvla_api
from nuvla.notifs.nuvla_api import Api as Nuvla

log = get_logger('matcher-event')


class EventSubsCfgMatcher:

    def __init__(self, event: Event):
        self._e = event
        self._trscm = TaggedResourceSubsCfgMatcher()

    def event_id(self) -> str:
        return self._e['id']

    def event_resource_id(self) -> str:
        return self._e.resource_id().split('_')[0]

    def resource_subscriptions(self, subs_cfgs: List[SubscriptionCfg]) -> \
            List[SubscriptionCfg]:
        return list(self._trscm.resource_subscriptions(self._e, subs_cfgs))

    #
    # BlackBox created

    def notif_build_blackbox(self,
                             sc: SubscriptionCfg) -> BlackboxEventNotification:
        return BlackboxEventNotification(sc, self._e)

    def is_event_blackbox_created(self):
        return self._e.content_match_href('^data-record/.*') and \
            self._e.content_is_state('created')

    def match_blackbox(self, subs_cfgs: List[SubscriptionCfg]) -> List[
            BlackboxEventNotification]:
        if not self.is_event_blackbox_created():
            return []

        res: List[BlackboxEventNotification] = []
        subs_on_resource = self.resource_subscriptions(subs_cfgs)
        if log.level == logging.DEBUG:
            log.debug('Active subscriptions on %s: %s',
                      self.event_id(), [x.get('id') for x in subs_on_resource])
        for sc in subs_on_resource:
            if log.level == logging.DEBUG:
                log.debug('Matching subscription %s on %s', sc.get("id"),
                          self.event_id())
            res.append(self.notif_build_blackbox(sc))

        return res

    #
    # module.publish

    MODULE_PUBLISHED_CRITERIA = 'module.publish'
    RESOURCE_KIND_APPSBOUQUET = 'apps-bouquet'
    RESOURCE_KIND_DEPLOYMENT = 'deployment'

    # predicates

    def is_event_module_published(self) -> bool:
        return self._e.is_name(self.MODULE_PUBLISHED_CRITERIA) and \
            self._e.is_successful()

    @classmethod
    def _is_event_module_publish_subscription(cls, subs_cfg: SubscriptionCfg) \
            -> bool:
        return subs_cfg.is_enabled() and \
            subs_cfg.criteria_metric() == 'name' and \
            subs_cfg.criteria_condition() == 'is' and \
            subs_cfg.criteria_value() == cls.MODULE_PUBLISHED_CRITERIA

    @classmethod
    def _is_event_module_publish_deployment_subscription(cls,
            subs_cfg: SubscriptionCfg) -> bool:
        return cls._is_event_module_publish_subscription(subs_cfg) and \
            subs_cfg.resource_kind() == cls.RESOURCE_KIND_DEPLOYMENT

    @classmethod
    def _is_event_module_publish_appsbouquet_subscription(cls,
            subs_cfg: SubscriptionCfg) -> bool:
        return cls._is_event_module_publish_subscription(subs_cfg) and \
            subs_cfg.resource_kind() == cls.RESOURCE_KIND_APPSBOUQUET

    # filters

    @classmethod
    def filter_event_module_publish_deployment_subscriptions(cls,
            subs_cfgs: List[SubscriptionCfg]) -> List[SubscriptionCfg]:
        return list(filter(
            cls._is_event_module_publish_deployment_subscription, subs_cfgs))

    @classmethod
    def filter_event_module_publish_appsbouquet_subscriptions(cls,
            subs_cfgs: List[SubscriptionCfg]) -> List[SubscriptionCfg]:
        return list(filter(
            cls._is_event_module_publish_appsbouquet_subscription, subs_cfgs))

    # API search methods

    @staticmethod
    def find_simple_deployments_by_application(nuvla: Nuvla, module_id: str,
            acl_owners: Set) -> List[dict]:
        """
        Given ID of the module of subtype 'application' and a set of owners,
        finds simple deployments that were started from this module and that
        belong to the owners.

        :param nuvla: initialised Nuvla API client
        :param module_id: str
        :param acl_owners: set of owners
        :return:
        """

        flt = f"module/id^='{module_id}' and acl/owners={list(acl_owners)} and " \
              f"deployment-set=null"
        select = 'id,acl'
        try:
            res = nuvla.search('deployment', filter=flt, select=select)
        except Exception as ex:
            log.exception('Failed getting deployments from Nuvla API server: %s',
                          exc_info=ex)
            return []

        if res:
            return [r.data for r in res.resources]
        return []

    @staticmethod
    def find_deployment_groups_by_application(nuvla: Nuvla, module_id: str,
            acl_owners: Set) -> List[dict]:
        """
        Only deployment groups that have "virtual" application sets are returned.

        :param nuvla: initialised Nuvla API client
        :param module_id:
        :param acl_owners:
        :return:
        """

        dpl_group_ids = []
        for owner in acl_owners:
            flt = f"module/id^='{module_id}' and acl/owners='{owner}' and " \
                  f"deployment-set!=null"
            select = ''
            aggregation = 'terms:deployment-set'
            try:
                res = nuvla.search('deployment', filter=flt, select=select,
                                   aggregation=aggregation)
            except Exception as ex:
                log.exception('Failed getting deployments from Nuvla API server: %s',
                              exc_info=ex)
                continue

            if not res:
                continue

            dpl_group_ids.extend(
                [b['key'] for b in res.data['aggregations']['terms:deployment-set']['buckets']])

        if not dpl_group_ids:
            return []

        dpl_groups_on_virt_app_set = []
        # per deployment group get all module 'application'-s from the application set
        for dpl_grp_id in dpl_group_ids:
            res = nuvla.get(dpl_grp_id, select='id,applications-sets,acl')
            dpl_grp = res.data
            for module in dpl_grp['applications-sets']:
                res = nuvla.get(module['id'], select='parent-path')
                # parent-path starting with 'apps-sets' indicates "virtual" app set
                if 'apps-sets' == res.data['parent-path']:
                    dpl_groups_on_virt_app_set.append(dpl_grp)

        return dpl_groups_on_virt_app_set

    @staticmethod
    def find_deployment_groups_by_application_set(nuvla: Nuvla, module_id: str,
            acl_owners: Set) -> List[dict]:
        """
        Given ID of the module of subtype 'applications_sets' and a set of owners,
        finds deployment groups that were started from this module and that
        belong to the owners.

        :param nuvla: initialised Nuvla API client
        :param module_id: str
        :param acl_owners: set of owners
        :return:
        """
        flt = f"applications-sets/id^='{module_id}' and acl/owners={list(acl_owners)}"
        select = 'id,acl'
        try:
            res = nuvla.search('deployment-set', filter=flt, select=select)
        except Exception as ex:
            log.exception('Failed getting deployment sets from Nuvla API server: %s',
                          exc_info=ex)
            return []

        if res:
            return [r.data for r in res.resources]
        return []


    @staticmethod
    def find_apps_bouquets_by_application(nuvla: Nuvla, app_id: str,
                                          acl_owners: Set) -> List[dict]:
        """
        Logic:
        1. find all module-applications-sets, which
           b. contain the searched application: applications-sets/applications/id='app_id'
        2. get all module applications_sets
           a. "subtype='applications_sets'", and
           b. not virtual, i.e. parent-path!='apps-sets', and
           c. owned by the users that subscribed for notifications: acl/owners='acl_owners'
        3. reconcile: find module applications_sets that are based on app_id
           via looking at the found module-applications-sets.

        :return: list of applications bouquets as dicts
        """

        # get module applications sets that contain the searched application
        flt = f"applications-sets/applications/id='{app_id}'"
        select = 'id'
        res = nuvla.search('module-applications-sets', filter=flt, select=select)
        if not res or (res and 0 == res.count):
            return []
        module_apps_sets_ids = [x.id for x in res.resources]

        # get all non-virtual apps the users own
        flt = f"subtype='applications_sets' and parent-path!='apps-sets' and " \
              f"acl/owners={list(acl_owners)}"
        select = 'id,name,path,versions,acl'
        res = nuvla.search('module', filter=flt, select=select)
        if not res or (res and 0 == res.count):
            return []

        apps_bqs: List[dict] = []

        for apps_bq in res.resources:
            if apps_bq.data['versions'][-1]['href'] in module_apps_sets_ids:
                apps_bqs.append(apps_bq.data)

        return apps_bqs

    # Helper methods.

    def get_module_subtype(self) -> Union[str, None]:
        content = self._e.resource_content()
        if content:
            return content.get('subtype')
        return None

    # Main logic.

    def deployments_from_module(self, module_id: str, acl_owners: Set) -> List[dict]:
        """
        Finds all types of deployments. The switch depends on module subtype.

        If the module is of a subtype 'application', then simple deployments and
        deployment-sets are searched.

        If the module is of a subtype 'applications-sets', then only
        deployment-sets are searched.

        :param module_id: str
        :param acl_owners: set of owners
        :return: list
        """
        module_subtype = self.get_module_subtype()

        if 'application' == module_subtype:
            return self.find_simple_deployments_by_application(module_id, acl_owners)

        if 'applications_sets' == module_subtype:
            return self.find_deployment_groups_by_application_set(module_id, acl_owners)

        log.warning('Unknown subtype %s on module %s', module_id, module_subtype)
        return []

    # Notification producers.

    def notifs_to_update_simple_deployments_from_app(self, nuvla: Nuvla, module_id: str,
            subs_cfgs: List[SubscriptionCfg]) -> \
            List[AppPublishedDeploymentsUpdateNotification]:
        """
        A.1
        Returns notifications with the filter for deployments for bulk update.

        :param nuvla: initialised Nuvla API client
        :param module_id: str
        :param subs_cfgs:
        :return:
        """

        log_msg = 'module published for simple deployments'
        subs_module_published = \
            self.filter_event_module_publish_deployment_subscriptions(subs_cfgs)
        if log.level == logging.DEBUG:
            log.debug('Active subscriptions on %s %s: %s', log_msg,
                      self.event_id(), [x.get('id') for x in subs_module_published])
        if not subs_module_published:
            return []

        notifs: List[AppPublishedDeploymentsUpdateNotification] = []
        acl_owners = collection_all_owners(subs_module_published)

        deployments = self.find_simple_deployments_by_application(
            nuvla, module_id, acl_owners)

        if not deployments:
            log.warning('No found on %s for %s', module_id, acl_owners)
            return []

        for sc in subs_module_published:
            log.debug('Matching subscription on %s on %s', log_msg, sc.get("id"),
                      self.event_id())
            # Are there deployments belonging to the owner of this subscription?
            depls_notify = False
            for app in deployments:
                if sc.owner() in app.get('acl', {}).get('owners', []):
                    depls_notify = True
                    break
            if depls_notify:
                notifs.append(
                    AppPublishedDeploymentsUpdateNotification(sc, self._e))

        return notifs

    def notifs_to_update_deployment_group_from_app(self, nuvla: Nuvla, module_id: str,
            subs_cfgs: List[SubscriptionCfg]) -> \
            List[AppAppBqPublishedDeploymentGroupUpdateNotification]:
        """
        A.2
        Returns individual notifications per deployment group.

        :param nuvla: initialised Nuvla API client
        :param module_id: str
        :param subs_cfgs:
        :return:
        """
        log_msg = 'application published for deployment groups'
        subs_module_published = \
            self.filter_event_module_publish_deployment_subscriptions(subs_cfgs)
        if log.level == logging.DEBUG:
            log.debug('Active subscriptions on %s %s: %s', log_msg,
                      self.event_id(), [x.get('id') for x in subs_module_published])
        if not subs_module_published:
            return []

        notifs: List[AppAppBqPublishedDeploymentGroupUpdateNotification] = []
        acl_owners = collection_all_owners(subs_module_published)

        dpl_groups_to_notify = self.find_deployment_groups_by_application(
            nuvla, module_id, acl_owners)

        if not dpl_groups_to_notify:
            log.warning('No deployment groups found on %s for %s', module_id, acl_owners)
            return []

        for sc in subs_module_published:
            log.debug('Matching subscription on %s on %s', log_msg, sc.get("id"),
                      self.event_id())
            # Are there apps bouquets belonging to the owner of this subscription?
            for dpl_grp in dpl_groups_to_notify:
                if sc.owner() in dpl_grp.get('acl', {}).get('owners', []):
                    notifs.append(
                        AppAppBqPublishedDeploymentGroupUpdateNotification(
                            dpl_grp['id'], sc, self._e))
        return notifs

    def notifs_to_update_apps_bouquets(self, nuvla: Nuvla, module_id: str,
                                       subs_cfgs: List[SubscriptionCfg]) -> \
            List[AppPublishedAppsBouquetUpdateNotification]:
        """
        A.3
        Returns individual notifications per applications bouquet.

        :param nuvla: initialised Nuvla API client
        :param module_id: str
        :param subs_cfgs:
        :return:
        """

        log_msg = 'module published for apps bouquet'
        subs_apps_bq_published = \
            self.filter_event_module_publish_appsbouquet_subscriptions(subs_cfgs)
        if log.level == logging.DEBUG:
            log.debug('Active subscriptions on %s %s: %s', log_msg,
                      self.event_id(), [x.get('id') for x in subs_apps_bq_published])
        if not subs_apps_bq_published:
            return []

        notifs: List[AppPublishedAppsBouquetUpdateNotification] = []
        acl_owners = collection_all_owners(subs_apps_bq_published)

        apps_to_notify = self.find_apps_bouquets_by_application(nuvla, module_id,
                                                                acl_owners)
        if not apps_to_notify:
            log.warning('No apps bouquets found on %s for %s', module_id, acl_owners)
            return []

        for sc in subs_apps_bq_published:
            log.debug('Matching subscription on %s on %s', log_msg, sc.get("id"),
                      self.event_id())
            # Are there apps bouquets belonging to the owner of this subscription?
            for app in apps_to_notify:
                if sc.owner() in app.get('acl', {}).get('owners', []):
                    notifs.append(
                        AppPublishedAppsBouquetUpdateNotification(
                            app.get('path'), sc, self._e))
        return notifs

    def notifs_to_update_deployment_group_from_app_bq(self, nuvla: Nuvla, module_id: str,
            subs_cfgs: List[SubscriptionCfg]) -> \
            List[AppAppBqPublishedDeploymentGroupUpdateNotification]:
        """
        B.1
        Returns individual notifications per deployment group.

        :param nuvla:
        :param module_id: application set module id
        :param subs_cfgs:
        :return:
        """

        log_msg = 'application bouquet published for deployment group'
        subs_apps_bq_published = \
            self.filter_event_module_publish_deployment_subscriptions(subs_cfgs)
        if log.level == logging.DEBUG:
            log.debug('Active subscriptions on %s %s: %s', log_msg,
                      self.event_id(), [x.get('id') for x in subs_apps_bq_published])
        if not subs_apps_bq_published:
            return []

        notifs: List[AppAppBqPublishedDeploymentGroupUpdateNotification] = []
        acl_owners = collection_all_owners(subs_apps_bq_published)

        dpls_to_notify = self.find_deployment_groups_by_application_set(
            nuvla, module_id, acl_owners)
        if not dpls_to_notify:
            log.warning('No deployment groups found on app bq %s for %s',
                        module_id, acl_owners)
            return []

        for sc in subs_apps_bq_published:
            log.debug('Matching subscription on %s on %s', log_msg, sc.get("id"),
                      self.event_id())
            # Are there apps bouquets belonging to the owner of this subscription?
            for dpl in dpls_to_notify:
                if sc.owner() in dpl.get('acl', {}).get('owners', []):
                    notifs.append(
                        AppAppBqPublishedDeploymentGroupUpdateNotification(
                            dpl['id'], sc, self._e))
        return notifs

    # Entry point.

    def match_module_published(self, subs_cfgs: List[SubscriptionCfg]) -> \
            List[AppPublishedDeploymentsUpdateNotification]:
        """
        There are two types of modules that can be published:
        * application
        * applications_sets

        A. When application gets published, three types of notifications are
        possible:
        1. simple deployment needs to be updated
        2. deployment group needs to be updated
        3. application bouquet needs to be updated

        B. When application bouquet gets published, single notification is
        possible:
        1. deployment group needs to be updated

        The following notifications will be produced:

        A.1 - user receives a link to UI Deployments page with all simple
              deployments pre-selected for a bulk update.
        A.2 - user receives a link to the concrete deployment group details page
              that needs to be updated. On the deployment group we need to
              highlight the application that triggered the notification as it
              might need attention.
        A.3 - same as A.2, but on the application bouquet details page.

        B.1 - user receives a link to the concrete deployment group that needs
              to be updated. In the deployment group we need to highlight the
              application bouquet that triggered the notification as it might
              need attention.

        :param subs_cfgs: list of subscriptions
        :return: list: notification objects
        """

        if not self.is_event_module_published():
            return []

        log.debug('Matching module publish event.')

        module_subtype = self.get_module_subtype()
        module_id = self.event_resource_id()

        nuvla = init_nuvla_api()

        notifs = []

        if 'application' == module_subtype:
            # A.1 simple deployment(s) need to be updated
            try:
                notifs.extend(
                    self.notifs_to_update_simple_deployments_from_app(
                        nuvla, module_id, subs_cfgs))
            except Exception as ex:
                log.exception('Failed reconciling for simple deployments on app: %s',
                              exc_info=ex)

            # A.2 deployment group needs to be updated
            try:
                notifs.extend(
                    self.notifs_to_update_deployment_group_from_app(
                        nuvla, module_id, subs_cfgs))
            except Exception as ex:
                log.exception('Failed reconciling for deployment groups on app: %s',
                              exc_info=ex)

            # A.3 subscription to Applications Bouquet.
            try:
                notifs.extend(
                    self.notifs_to_update_apps_bouquets(nuvla, module_id, subs_cfgs))
            except Exception as ex:
                log.exception('Failed reconciling for application bouquets on app: %s',
                              exc_info=ex)

        if 'applications_sets' == module_subtype:
            # B.1 deployment group needs to be updated
            try:
                notifs.extend(
                    self.notifs_to_update_deployment_group_from_app_bq(
                        nuvla, module_id, subs_cfgs))
            except Exception as ex:
                log.exception('Failed reconciling for deployment groups on app bouquet: %s',
                              exc_info=ex)

        return notifs
