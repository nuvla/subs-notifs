"""
This module provides implementations for matching criteria defined in
user notification subscriptions against the metrics coming from different
components of Nuvla the user has access to.
"""

import traceback
from typing import Dict, Union, List

from nuvla.notifs.db import RxTxDB, gb_to_bytes, bytes_to_gb
from nuvla.notifs.log import get_logger
from nuvla.notifs.metric import NuvlaEdgeMetrics, MetricNotFound
from nuvla.notifs.event import Event
from nuvla.notifs.resource import Resource
from nuvla.notifs.notification import NuvlaEdgeNotificationBuilder, \
    NuvlaEdgeNotification, BlackboxEventNotification
from nuvla.notifs.subscription import SubscriptionCfg

log = get_logger('matcher')


def gt(val_bytes: Union[int, float], sc: SubscriptionCfg) -> bool:
    """greater than"""
    return val_bytes > gb_to_bytes(sc.criteria_value())


def lt(val_bytes: Union[int, float], sc: SubscriptionCfg) -> bool:
    """less than"""
    return val_bytes < gb_to_bytes(sc.criteria_value())


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
        if super().resource_subscribed(resource, subs_cfg, with_disabled):
            if subs_cfg.is_tags_set():
                return subs_cfg.tags_match(
                    resource.get('tags', resource.get('TAGS', [])))
            return True
        return False


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

    def _load_moved_above_thld(self, sc: SubscriptionCfg) -> bool:
        return self._m.load_pct_prev() <= sc.criteria_value() < \
               self._m.load_pct_curr()

    def _load_moved_below_thld(self, sc: SubscriptionCfg) -> bool:
        return self._m.load_pct_curr() < sc.criteria_value() <= \
               self._m.load_pct_prev()

    def _load_moved_over_thld(self, sc: SubscriptionCfg) -> bool:
        return self._load_moved_above_thld(sc) or \
               self._load_moved_below_thld(sc)

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

    def get_rxtx(self, subs_cfg: SubscriptionCfg, dev_name, kind) -> \
            Union[None, int]:
        if self._net_db is None:
            log.debug('network db not defined ... return')
            return None
        if kind == 'rx':
            return self._net_db.get_rx(subs_cfg['id'], self.metrics_id(),
                                       dev_name)
        if kind == 'tx':
            return self._net_db.get_tx(subs_cfg['id'], self.metrics_id(),
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

        if sc is None or len(sc) < 1:
            log.debug('Subscription configuration not defined ... return')
            return None

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
            val = self.get_rxtx(sc, dev_name, kind)
        except KeyError as ex:
            log.warning('No such key %s when getting rxtx from db', ex)
            return None

        if val is None:
            return None

        if gt(val, sc) and not self.is_rxtx_above_thld(sc, dev_name, kind):
            self.set_rxtx_above_thld(sc, dev_name, kind)
            return {'interface': dev_name, 'value': bytes_to_gb(val)}

        # 'above threshold' event was already registered, but then the
        # threshold in the subscription configuration was increased.
        if self.is_rxtx_above_thld(sc, dev_name, kind) and lt(val, sc):
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

    def _notif_build_net_rxtx(self, sc: SubscriptionCfg,
                              res_m: dict, kind: str) -> NuvlaEdgeNotification:
        """Current value of the metric is provided in Bytes. The user defined
        threshold is in Gb. Adjustment for a proper current value in Gb for the
        notification message are applied."""
        return NuvlaEdgeNotificationBuilder(sc, self._m) \
            .metric_name(f'{res_m["interface"]} {kind.capitalize()} above') \
            .value_rxtx_adjusted(res_m['value']) \
            .recovery(res_m.get('recovery', False)) \
            .build()

    def notif_build_net_rx(self, sc: SubscriptionCfg,
                           res_m: dict) -> NuvlaEdgeNotification:
        return self._notif_build_net_rxtx(sc, res_m, 'rx')

    def notif_build_net_tx(self, sc: SubscriptionCfg,
                           res_m: dict) -> NuvlaEdgeNotification:
        return self._notif_build_net_rxtx(sc, res_m, 'tx')

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
            .timestamp_as_nuvla_timestamp() \
            .build()

    def resource_subscriptions(self, subs_cfgs: List[SubscriptionCfg]) -> \
            List[SubscriptionCfg]:
        return list(self._trscm.resource_subscriptions(self._m, subs_cfgs))

    def matchers(self):
        return {
            # network Rx
            'network_rx_above_thld':
                {'matcher': self.network_rx_above_thld,
                 'notif-builder': self.notif_build_net_rx},
            # network Tx
            'network_tx_above_thld':
                {'matcher': self.network_tx_above_thld,
                 'notif-builder': self.notif_build_net_tx},
            # CPU load
            'match_load':
                {'matcher': self.match_load,
                 'notif-builder': self.notif_build_load},
            # RAM
            'match_ram':
                {'matcher': self.match_ram,
                 'notif-builder': self.notif_build_ram},
            # Disk
            'match_disk':
                {'matcher': self.match_disk,
                 'notif-builder': self.notif_build_disk},
            # Online
            'match_online':
                {'matcher': self.match_online,
                 'notif-builder': self.notif_build_online}
        }

    def match_all(self, subs_cfgs: List[SubscriptionCfg]) -> List[Dict]:
        res = []
        subs_on_resource = self.resource_subscriptions(subs_cfgs)
        log.debug('Active subscriptions on %s: %s',
                  self.metrics_id(), [x.get('id') for x in subs_on_resource])
        for sc in subs_on_resource:
            log.debug('Matching subscription %s on %s', sc.get("id"),
                      self.metrics_id())
            for m_name, matcher in self.matchers().items():
                try:
                    res_m = matcher['matcher'](sc)
                except Exception as ex:
                    log.error('Failed matching %s: %s', m_name, ex)
                    traceback.print_exc()
                    continue
                log.debug('%s... %s', m_name, res_m)
                if res_m:
                    log.debug('Condition matched: %s', sc)
                    try:
                        res.append(matcher['notif-builder'](sc, res_m))
                    except Exception as ex:
                        log.error('Failed building notification for %s: %s',
                                  m_name, ex)
                        traceback.print_exc()

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
