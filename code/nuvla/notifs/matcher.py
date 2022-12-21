from typing import Dict, Union, List, Iterator

from nuvla.notifs.db import RxTxDB
from nuvla.notifs.log import get_logger
from nuvla.notifs.metric import ResourceMetrics, NuvlaEdgeResourceMetrics, \
    MetricNotFound
from nuvla.notifs.notification import NuvlaEdgeNotificationBuilder, \
    NuvlaEdgeNotification
from nuvla.notifs.subscription import SubscriptionConfig

log = get_logger('matcher')


class SubscriptionConfigMatcher:
    def __init__(self, metrics: ResourceMetrics):
        self.m = metrics

    def resource_subscribed(self, subs_conf: SubscriptionConfig) -> bool:
        log.debug('resource_subscribed: SubscriptionConfig %s', subs_conf)
        return subs_conf.is_enabled() and \
               subs_conf.can_view_resource(self.m.get('acl', self.m.get('ACL', {}))) and \
               subs_conf.tags_match(self.m.get('tags', self.m.get('TAGS', [])))

    def resource_subscriptions(self, subs_confs: List[SubscriptionConfig]) -> \
            Iterator[SubscriptionConfig]:
        return filter(self.resource_subscribed, subs_confs)

    def metrics(self) -> ResourceMetrics:
        return self.m

    def metrics_id(self) -> str:
        return self.m.get('id')


def metric_not_found_ex_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except MetricNotFound as ex:
            log.warning('Metric not found due to: %s', ex)
            return None
    return wrapper


class NuvlaEdgeSubsConfMatcher(SubscriptionConfigMatcher):
    """
    Given `metrics` from NuvlaEdge, matches them against user provided criteria
    defined in subscription configs.
    """

    MATCHED = {'matched': True,
               'recovery': False}
    MATCHED_RECOVERY = {'matched': True,
                        'recovery': True}

    def __init__(self, metrics: NuvlaEdgeResourceMetrics,
                 net_db: Union[None, RxTxDB] = None):
        super().__init__(metrics)
        self._net_db: RxTxDB = net_db

    def _load_moved_above_thld(self, sc: SubscriptionConfig):
        return self.m.load_pct_prev() <= sc.criteria_value() < self.m.load_pct_curr()

    def _load_moved_below_thld(self, sc: SubscriptionConfig):
        return self.m.load_pct_curr() < sc.criteria_value() <= self.m.load_pct_prev()

    def _load_moved_over_thld(self, sc: SubscriptionConfig):
        return self._load_moved_above_thld(sc) or self._load_moved_below_thld(sc)

    @metric_not_found_ex_handler
    def match_load(self, sc: SubscriptionConfig) -> Union[None, Dict[str, bool]]:
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

    def _ram_moved_above_thld(self, sc: SubscriptionConfig):
        return self.m.ram_pct_prev() <= sc.criteria_value() < self.m.ram_pct_curr()

    def _ram_moved_below_thld(self, sc: SubscriptionConfig):
        return self.m.ram_pct_curr() < sc.criteria_value() <= self.m.ram_pct_prev()

    @metric_not_found_ex_handler
    def match_ram(self, sc: SubscriptionConfig) -> Union[None, Dict[str, bool]]:
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

    def disk_pct_curr(self, sc: SubscriptionConfig):
        return self.m.disk_pct_curr(sc.criteria_dev_name())

    def disk_pct_prev(self, sc: SubscriptionConfig):
        return self.m.disk_pct_prev(sc.criteria_dev_name())

    def _disk_moved_above_thld(self, sc: SubscriptionConfig) -> Union[None, float]:
        prev = self.disk_pct_prev(sc)
        curr = self.disk_pct_curr(sc)
        if prev and curr:
            return prev <= sc.criteria_value() < curr

    def _disk_moved_below_thld(self, sc: SubscriptionConfig):
        prev = self.disk_pct_prev(sc)
        curr = self.disk_pct_curr(sc)
        if prev and curr:
            return curr < sc.criteria_value() <= prev

    @metric_not_found_ex_handler
    def match_disk(self, sc: SubscriptionConfig) -> Union[None, Dict[str, bool]]:
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

    def network_rx_above_thld(self, sc: SubscriptionConfig) -> Union[None, Dict]:
        return self._network_rxtx_above_thld(sc, 'rx')

    def network_tx_above_thld(self, sc: SubscriptionConfig) -> Union[None, Dict]:
        return self._network_rxtx_above_thld(sc, 'tx')

    def _network_rxtx_above_thld(self, sc: SubscriptionConfig, kind: str) -> Union[None, Dict]:

        def ge(val, sc: SubscriptionConfig) -> bool:
            """greater than or equal"""
            return val and val >= sc.criteria_value()

        def le(val, sc: SubscriptionConfig) -> bool:
            """less than or equal"""
            return val and val <= sc.criteria_value()

        if not sc.is_metric_cond(f'network-{kind}', '>'):
            return None
        m: NuvlaEdgeResourceMetrics = self.metrics()
        if m:
            dev_name = sc.criteria_dev_name() or m.default_gw_name()
            if not dev_name:
                return None
            try:
                if self._net_db is not None:
                    val = getattr(self._net_db, f'get_{kind}_gb')(m['id'], dev_name)
                else:
                    return None
            except KeyError:
                return None
            if ge(val, sc) and \
                    not self._net_db.get_above_thld(sc['id'], m['id'], dev_name, kind):
                self._net_db.set_above_thld(sc['id'], m['id'], dev_name, kind)
                return {'interface': dev_name, 'value': val}
            if le(val, sc) and \
                    self._net_db.get_above_thld(sc['id'], m['id'], dev_name, kind):
                self._net_db.reset_above_thld(sc['id'], m['id'], dev_name, kind)

        return None

    def _went_offline(self):
        if self.m.get('ONLINE_PREV') and not self.m.get('ONLINE'):
            return True
        return False

    def _went_online(self):
        if self.m.get('ONLINE') and not self.m.get('ONLINE_PREV'):
            return True
        return False

    def match_online(self, sc: SubscriptionConfig) -> Union[None, Dict]:
        if sc.is_metric_cond('state', 'no'):
            if self._went_offline():
                return self.MATCHED
            if self._went_online():
                return self.MATCHED_RECOVERY
        return None

    def notif_build_net_rx(self, sc: SubscriptionConfig, res_m) -> NuvlaEdgeNotification:
        return NuvlaEdgeNotificationBuilder(sc, self.metrics()) \
            .name(f'{res_m["interface"]} Rx above') \
            .value(res_m['value']) \
            .recovery(res_m.get('recovery', False)) \
            .build()

    def notif_build_net_tx(self, sc: SubscriptionConfig, res_m) -> NuvlaEdgeNotification:
        return NuvlaEdgeNotificationBuilder(sc, self.metrics()) \
            .name(f'{res_m["interface"]} Tx above') \
            .value(res_m['value']) \
            .recovery(res_m.get('recovery', False)) \
            .build()

    def notif_build_load(self, sc: SubscriptionConfig, res_m) -> NuvlaEdgeNotification:
        return NuvlaEdgeNotificationBuilder(sc, self.metrics()) \
            .name('NE load %') \
            .value(self.metrics().load_pct_curr()) \
            .recovery(res_m.get('recovery', False)) \
            .build()

    def notif_build_ram(self, sc: SubscriptionConfig, res_m) -> NuvlaEdgeNotification:
        return NuvlaEdgeNotificationBuilder(sc, self.metrics()) \
            .name('NE ram %') \
            .value(self.metrics().ram_pct_curr()) \
            .recovery(res_m.get('recovery', False)) \
            .build()

    def notif_build_disk(self, sc: SubscriptionConfig, res_m) -> NuvlaEdgeNotification:
        return NuvlaEdgeNotificationBuilder(sc, self.metrics()) \
            .name('NE disk %') \
            .value(self.metrics().disk_pct_curr(sc)) \
            .recovery(res_m.get('recovery', False)) \
            .build()

    def notif_build_online(self, sc: SubscriptionConfig, res_m) -> NuvlaEdgeNotification:
        return NuvlaEdgeNotificationBuilder(sc, self.metrics()) \
            .name('NE online') \
            .condition(str(res_m.get('recovery', False)).lower()) \
            .recovery(res_m.get('recovery', False)) \
            .value('') \
            .condition_value('') \
            .build()

    def match_all(self, subs_confs: List[SubscriptionConfig]) -> List[Dict]:
        res = []
        subs_on_resource = list(self.resource_subscriptions(subs_confs))
        log.debug('Active subscriptions on %s: %s',
                  self.metrics_id(), [x.get('id') for x in subs_on_resource])
        for sc in subs_on_resource:
            log.debug('Matching subscription %s on %s', sc.get("id"), self.metrics_id())
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
