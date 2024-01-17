import traceback
from typing import Union, Dict, List

from nuvla.notifs.db.driver import RxTxDB
from nuvla.notifs.log import get_logger
from nuvla.notifs.matching.base import gt, lt, TaggedResourceSubsCfgMatcher
from nuvla.notifs.models.metric import MetricNotFound, NuvlaEdgeMetrics
from nuvla.notifs.models.subscription import SubscriptionCfg, \
    NETWORK_METRIC_PREFIX
from nuvla.notifs.notification import NuvlaEdgeNotification, \
    NuvlaEdgeNotificationBuilder

log = get_logger('matcher-ne_telelem')

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

    _MSG_NET_DB_NOT_DEFINED = 'network db not defined ... return'

    def get_rxtx(self, subs_cfg: SubscriptionCfg, dev_name, kind) -> \
            Union[None, int]:
        if self._net_db is None:
            log.debug(self._MSG_NET_DB_NOT_DEFINED)
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
            log.debug(self._MSG_NET_DB_NOT_DEFINED)
            return False
        return self._net_db.get_above_thld(sc['id'], self.metrics_id(), iface,
                                           kind)

    def set_rxtx_above_thld(self, sc: SubscriptionCfg, iface, kind):
        if self._net_db is None:
            log.debug(self._MSG_NET_DB_NOT_DEFINED)
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
            return {'interface': dev_name, 'value': val}

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

    def _from_heartbeat(self):
        """Returns True if metrics are from heartbeat.

        The heuristics to know if the metrics are from heartbeat is to check if
        the there are metrics under 'RESOURCES' key. If there are any, then this
        means that the metrics came from the telemetry delivery loop. Otherwise,
        the metrics are from the heartbeat event.

        NB! This is a very weak heuristic, but it is the best we can do for now.
        """
        if NuvlaEdgeMetrics.RESOURCES_KEY in self._m and self._m.get(
                NuvlaEdgeMetrics.RESOURCES_KEY) is not None:
            return False
        return True

    def match_online(self, sc: SubscriptionCfg) -> Union[None, Dict]:
        # FIXME: a temporary solution to avoid double online notification.
        if not self._from_heartbeat():
            return None
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
