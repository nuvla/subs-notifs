"""
This module provides implementations for matching criteria defined in
user notification subscriptions against the metrics coming from different
components of Nuvla the user has access to.
"""

from typing import Dict, Union, List

from nuvla.notifs.db import RxTxDB
from nuvla.notifs.log import get_logger
from nuvla.notifs.metric import NuvlaEdgeMetrics, MetricNotFound
from nuvla.notifs.event import Event
from nuvla.notifs.resource import Resource
from nuvla.notifs.notification import NuvlaEdgeNotificationBuilder, \
    NuvlaEdgeNotification, BlackboxEventNotification
from nuvla.notifs.subscription import SubscriptionCfg

log = get_logger('matcher')


def ge(val, sc: SubscriptionCfg) -> bool:
    """greater than or equal"""
    return val and val >= sc.criteria_value()


def le(val, sc: SubscriptionCfg) -> bool:
    """less than or equal"""
    return val and val <= sc.criteria_value()


NETWORK_METRIC_PREFIX = 'network-'


class ResourceSubsCfgMatcher:
    """
    Helper methods to check if subscription configurations are subscribed to
    a resource.
    """

    @classmethod
    def resource_subscribed(cls, resource: Resource,
                            subs_cfg: SubscriptionCfg,
                            with_disabled=False) -> bool:
        if with_disabled:
            return subs_cfg.can_view_resource(
                resource.get('acl', resource.get('ACL', {})))
        return subs_cfg.is_enabled() and \
               subs_cfg.can_view_resource(
                   resource.get('acl', resource.get('ACL', {})))

    @classmethod
    def resource_subscriptions(cls, resource: Resource,
                               subs_cfgs: List[SubscriptionCfg]) -> \
            List[SubscriptionCfg]:
        subs_cfgs_subscribed = []
        for subs_cfg in subs_cfgs:
            if cls.resource_subscribed(resource, subs_cfg):
                subs_cfgs_subscribed.append(subs_cfg)
        return subs_cfgs_subscribed

    @classmethod
    def resource_subscriptions_ids(cls, resource: Resource,
                                   subs_cfgs: List[SubscriptionCfg]) \
            -> List[str]:
        return [sc['id'] for sc in
                cls.resource_subscriptions(resource, subs_cfgs)]


class TaggedResourceSubsCfgMatcher(ResourceSubsCfgMatcher):
    """
    Helper methods to check if subscription configurations are subscribed to
    a tagged resource.
    """

    def __init__(self):
        super().__init__()

    @classmethod
    def resource_subscribed(cls, resource: Resource,
                            subs_cfg: SubscriptionCfg,
                            with_disabled=False) -> bool:
        return super().resource_subscribed(resource, subs_cfg,
                                           with_disabled) and \
               subs_cfg.tags_match(
                   resource.get('tags', resource.get('TAGS', [])))


class TaggedResourceNetworkSubsCfgMatcher(TaggedResourceSubsCfgMatcher):
    """
    Helper methods to check if subscription configurations are subscribed to
    networking metrics on a tagged resource.
    """

    def __init__(self):
        super().__init__()

    @classmethod
    def resource_subscribed(cls, resource: Resource,
                            subs_cfg: SubscriptionCfg,
                            with_disabled=True) -> bool:
        return super().resource_subscribed(resource, subs_cfg,
                                           with_disabled) and \
               subs_cfg.criteria_metric().startswith(NETWORK_METRIC_PREFIX)


def metric_not_found_ex_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except MetricNotFound as ex:
            log.warning('Metric not found due to: %s', ex)
            return None

    return wrapper


class NuvlaEdgeSubsCfgMatcher:
    """
    Given `metrics` from NuvlaEdge, matches them against user provided criteria
    defined in subscription configs.
    """

    MATCHED = {'matched': True,
               'recovery': False}
    MATCHED_RECOVERY = {'matched': True,
                        'recovery': True}

    def __init__(self, metrics: NuvlaEdgeMetrics,
                 net_db: Union[None, RxTxDB] = None):
        self._m: NuvlaEdgeMetrics = metrics
        self._net_db: RxTxDB = net_db
        self._trscm = TaggedResourceSubsCfgMatcher()

    def metrics_id(self) -> str:
        return self._m.get('id')

    def _load_moved_above_thld(self, sc: SubscriptionCfg):
        return self._m.load_pct_prev() <= sc.criteria_value() < self._m.load_pct_curr()

    def _load_moved_below_thld(self, sc: SubscriptionCfg):
        return self._m.load_pct_curr() < sc.criteria_value() <= self._m.load_pct_prev()

    def _load_moved_over_thld(self, sc: SubscriptionCfg):
        return self._load_moved_above_thld(sc) or self._load_moved_below_thld(
            sc)

    @metric_not_found_ex_handler
    def match_load(self, sc: SubscriptionCfg) -> Union[
        None, Dict[str, bool]]:
        if sc.is_metric_cond('load', '>'):
            if self._load_moved_above_thld(sc):
                return self.MATCHED
            if self._load_moved_below_thld(sc):
                return self.MATCHED_RECOVERY
        elif sc.is_metric_cond('load', '<'):
            if self._load_moved_below_thld(sc):
                return self.MATCHED
            if self._load_moved_above_thld(sc):
                return self.MATCHED_RECOVERY
        return None

    def _ram_moved_above_thld(self, sc: SubscriptionCfg):
        return self._m.ram_pct_prev() <= sc.criteria_value() < self._m.ram_pct_curr()

    def _ram_moved_below_thld(self, sc: SubscriptionCfg):
        return self._m.ram_pct_curr() < sc.criteria_value() <= self._m.ram_pct_prev()

    @metric_not_found_ex_handler
    def match_ram(self, sc: SubscriptionCfg) -> Union[None, Dict[str, bool]]:
        if sc.is_metric_cond('ram', '>'):
            if self._ram_moved_above_thld(sc):
                return self.MATCHED
            if self._ram_moved_below_thld(sc):
                return self.MATCHED_RECOVERY
        elif sc.is_metric_cond('ram', '<'):
            if self._ram_moved_below_thld(sc):
                return self.MATCHED
            if self._ram_moved_above_thld(sc):
                return self.MATCHED_RECOVERY
        return None

    def disk_pct_curr(self, sc: SubscriptionCfg):
        return self._m.disk_pct_curr(sc.criteria_dev_name())

    def disk_pct_prev(self, sc: SubscriptionCfg):
        return self._m.disk_pct_prev(sc.criteria_dev_name())

    def _disk_moved_above_thld(self, sc: SubscriptionCfg) -> Union[
        None, float]:
        prev = self.disk_pct_prev(sc)
        curr = self.disk_pct_curr(sc)
        if prev and curr:
            return prev <= sc.criteria_value() < curr

    def _disk_moved_below_thld(self, sc: SubscriptionCfg):
        prev = self.disk_pct_prev(sc)
        curr = self.disk_pct_curr(sc)
        if prev and curr:
            return curr < sc.criteria_value() <= prev

    @metric_not_found_ex_handler
    def match_disk(self, sc: SubscriptionCfg) -> Union[
        None, Dict[str, bool]]:
        if sc.is_metric_cond('disk', '>'):
            if self._disk_moved_above_thld(sc):
                return self.MATCHED
            if self._disk_moved_below_thld(sc):
                return self.MATCHED_RECOVERY
        elif sc.is_metric_cond('disk', '<'):
            if self._disk_moved_below_thld(sc):
                return self.MATCHED
            if self._disk_moved_above_thld(sc):
                return self.MATCHED_RECOVERY
        return None

    def network_rx_above_thld(self, sc: SubscriptionCfg) -> Union[None, Dict]:
        return self._network_rxtx_above_thld(sc, 'rx')

    def network_tx_above_thld(self, sc: SubscriptionCfg) -> Union[None, Dict]:
        return self._network_rxtx_above_thld(sc, 'tx')

    def get_rxtx_gb(self, subs_cfg: SubscriptionCfg, dev_name, kind) -> \
            Union[None, int]:
        if self._net_db is None:
            log.debug('network db not defined ... return')
            return None
        if kind == 'rx':
            return self._net_db.get_rx_gb(subs_cfg['id'], self.metrics_id(),
                                          dev_name)
        if kind == 'tx':
            return self._net_db.get_tx_gb(subs_cfg['id'], self.metrics_id(),
                                          dev_name)
        return None

    def is_rxtx_above_thld(self, sc: SubscriptionCfg, iface, kind) -> bool:
        if self._net_db is None:
            log.debug('net db not defined ... return')
            return False
        return self._net_db.get_above_thld(sc['id'], self.metrics_id(), iface,
                                           kind)

    def set_rxtx_above_thld(self, sc: SubscriptionCfg, iface, kind):
        if self._net_db is None:
            log.debug('net db not defined ... return')
            return
        self._net_db.set_above_thld(sc['id'], self.metrics_id(), iface, kind)

    def reset_rxtx_above_thld(self, sc: SubscriptionCfg, iface, kind):
        if self._net_db is None:
            log.debug('net db not defined ... return')
            return
        self._net_db.reset_above_thld(sc['id'], self.metrics_id(), iface, kind)

    def network_device_name(self, sc: SubscriptionCfg) -> Union[None, str]:
        return sc.criteria_dev_name() or self._m.default_gw_name()

    def _network_rxtx_above_thld(self, sc: SubscriptionCfg, kind: str) -> \
            Union[None, Dict]:

        if kind not in ['rx', 'tx']:
            msg = f'Incorrect kind {kind}'
            log.error(msg)
            raise ValueError(msg)

        if not sc.is_metric_cond(f'{NETWORK_METRIC_PREFIX}{kind}', '>'):
            log.debug('Metric condition is not %s%s ">" ... return',
                      NETWORK_METRIC_PREFIX, kind)
            return None

        if not self._m:
            log.debug('Metrics not defined ... return')
            return None

        dev_name = self.network_device_name(sc)
        if not dev_name:
            log.debug('No device name from criteria or metrics ... return')
            return None

        try:
            val = self.get_rxtx_gb(sc, dev_name, kind)
        except KeyError as ex:
            log.warning('No such key %s when getting rxtx from db', ex)
            return None

        if ge(val, sc) and not self.is_rxtx_above_thld(sc, dev_name, kind):
            self.set_rxtx_above_thld(sc, dev_name, kind)
            return {'interface': dev_name, 'value': val}

        if le(val, sc) and self.is_rxtx_above_thld(sc, dev_name, kind):
            self.reset_rxtx_above_thld(sc, dev_name, kind)

    def _went_offline(self):
        if self._m.get('ONLINE_PREV') and not self._m.get('ONLINE'):
            return True
        return False

    def _went_online(self):
        if self._m.get('ONLINE') and not self._m.get('ONLINE_PREV'):
            return True
        return False

    def match_online(self, sc: SubscriptionCfg) -> Union[None, Dict]:
        if sc.is_metric_cond('state', 'no'):
            if self._went_offline():
                return self.MATCHED
            if self._went_online():
                return self.MATCHED_RECOVERY
        return None

    def notif_build_net_rx(self, sc: SubscriptionCfg,
                           res_m: dict) -> NuvlaEdgeNotification:
        return NuvlaEdgeNotificationBuilder(sc, self._m) \
            .metric_name(f'{res_m["interface"]} Rx above') \
            .value(res_m['value']) \
            .recovery(res_m.get('recovery', False)) \
            .build()

    def notif_build_net_tx(self, sc: SubscriptionCfg,
                           res_m: dict) -> NuvlaEdgeNotification:
        return NuvlaEdgeNotificationBuilder(sc, self._m) \
            .metric_name(f'{res_m["interface"]} Tx above') \
            .value(res_m['value']) \
            .recovery(res_m.get('recovery', False)) \
            .build()

    def notif_build_load(self, sc: SubscriptionCfg,
                         res_m: dict) -> NuvlaEdgeNotification:
        return NuvlaEdgeNotificationBuilder(sc, self._m) \
            .metric_name(sc.criteria_metric()) \
            .recovery(res_m.get('recovery', False)) \
            .value(self._m.load_pct_curr()) \
            .build()

    def notif_build_ram(self, sc: SubscriptionCfg,
                        res_m: dict) -> NuvlaEdgeNotification:
        return NuvlaEdgeNotificationBuilder(sc, self._m) \
            .metric_name(sc.criteria_metric()) \
            .recovery(res_m.get('recovery', False)) \
            .value(self._m.ram_pct_curr()) \
            .build()

    def notif_build_disk(self, sc: SubscriptionCfg,
                         res_m: dict) -> NuvlaEdgeNotification:
        return NuvlaEdgeNotificationBuilder(sc, self._m) \
            .metric_name(sc.criteria_metric()) \
            .recovery(res_m.get('recovery', False)) \
            .value(self._m.disk_pct_curr(sc.criteria_dev_name())) \
            .build()

    def notif_build_online(self, sc: SubscriptionCfg,
                           res_m: dict) -> NuvlaEdgeNotification:
        return NuvlaEdgeNotificationBuilder(sc, self._m) \
            .metric_name('NE online') \
            .condition(str(res_m.get('recovery', False)).lower()) \
            .recovery(res_m.get('recovery', False)) \
            .value('') \
            .condition_value('') \
            .build()

    def resource_subscriptions(self, subs_cfgs: List[SubscriptionCfg]) -> \
            List[SubscriptionCfg]:
        return list(self._trscm.resource_subscriptions(self._m, subs_cfgs))

    def match_all(self, subs_cfgs: List[SubscriptionCfg]) -> List[Dict]:
        res = []
        subs_on_resource = self.resource_subscriptions(subs_cfgs)
        log.debug('Active subscriptions on %s: %s',
                  self.metrics_id(), [x.get('id') for x in subs_on_resource])
        for sc in subs_on_resource:
            log.debug('Matching subscription %s on %s', sc.get("id"),
                      self.metrics_id())
            # network Rx
            res_m = self.network_rx_above_thld(sc)
            log.debug('network_rx_above_thld... %s', res_m)
            if res_m:
                log.debug('Condition matched: %s', sc)
                res.append(self.notif_build_net_rx(sc, res_m))
            # network Tx
            res_m = self.network_tx_above_thld(sc)
            log.debug('network_tx_above_thld... %s', res_m)
            if res_m:
                log.debug('Condition matched: %s', sc)
                res.append(self.notif_build_net_tx(sc, res_m))
            # CPU load
            res_m = self.match_load(sc)
            log.debug('match_load... %s', res_m)
            if res_m:
                log.debug('Condition matched: %s', sc)
                res.append(self.notif_build_load(sc, res_m))
            # RAM
            res_m = self.match_ram(sc)
            log.debug('match_ram... %s', res_m)
            if res_m:
                log.debug('Condition matched: %s', sc)
                res.append(self.notif_build_ram(sc, res_m))
            # Disk
            res_m = self.match_disk(sc)
            log.debug('match_disk... %s', res_m)
            if res_m:
                log.debug('Condition matched: %s', sc)
                res.append(self.notif_build_disk(sc, res_m))
            # Online
            res_m = self.match_online(sc)
            log.debug('match_online... %s', res_m)
            if res_m:
                log.debug('Condition matched: %s', sc)
                res.append(self.notif_build_online(sc, res_m))
        return res


class EventSubsCfgMatcher:

    def __init__(self, event: Event):
        self._e = event
        self._trscm = TaggedResourceSubsCfgMatcher()

    def event_id(self):
        return self._e['id']

    def resource_subscriptions(self, subs_cfgs: List[SubscriptionCfg]) -> \
            List[SubscriptionCfg]:
        return list(self._trscm.resource_subscriptions(self._e, subs_cfgs))

    def notif_build_blackbox(self,
                             sc: SubscriptionCfg) -> BlackboxEventNotification:
        return BlackboxEventNotification(sc, self._e)

    def match_blackbox(self, subs_cfgs: List[SubscriptionCfg]) -> List[
            BlackboxEventNotification]:
        res: List[BlackboxEventNotification] = []
        subs_on_resource = self.resource_subscriptions(subs_cfgs)
        log.debug('Active subscriptions on %s: %s',
                  self.event_id(), [x.get('id') for x in subs_on_resource])
        for sc in subs_on_resource:
            log.debug('Matching subscription %s on %s', sc.get("id"),
                      self.event_id())
            if self._e.content_match_href('^data-record/.*') and \
                    self._e.content_is_state('created'):
                res.append(self.notif_build_blackbox(sc))

        return res
