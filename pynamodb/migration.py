"""
Contains helpers to assist in "migrations" from one version of
PynamoDB to the next, in cases where breaking changes have happened.
"""

import logging

from botocore.exceptions import ClientError
from pynamodb.exceptions import UpdateError
from pynamodb.expressions.operand import Path

log = logging.getLogger(__name__)


def _build_lba_filter_condition(attribute_names):
    """
    Build a filter condition suitable for passing to scan/rate_limited_scan, which
    will filter out any items for which none of the given attributes have native
    DynamoDB type of 'N'.
    """
    int_filter_condition = None
    for attr_name in attribute_names:
        if int_filter_condition is None:
            int_filter_condition = Path(attr_name).is_type('N')
        else:
            int_filter_condition |= Path(attr_name).is_type('N')

    return int_filter_condition


def migrate_boolean_attributes(model_class,
                               attribute_names,
                               read_capacity_to_consume_per_second=10,
                               unit_testing=False,
                               mock_conditional_update_failure=False):
    """
    Migrates boolean attributes per GitHub issue 404.

    For context, see https://github.com/pynamodb/PynamoDB/issues/404

    Scan through all items for the given model (using
    `rate_limited_scan`) and use `update()` to re-set any attributes
    given in attribute_names.

    All attribute names must signify attributes that are of type BooleanAttribute.

    Attributes that are None are ignored.

    The scan is rate limited as a result of our use of
    `rate_limited_scan`, but the subsequent writes will also consume
    capacity. Therefor, the value passed to
    read_capacity_to_consume_per_second must take into account the
    fact that writes will be generated. The caller should assume every
    object found is updated, and consider the provisioned *write*
    capacity.

    See http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ProvisionedThroughput.html

    Note that read and write capacity units are not directly 1:1 comparable.

    Returns: (num_items_visited, num_items_changed)
    """
    log.info('migrating items; no progress will be reported until completed; this may take a while')
    num_items_with_actions = 0
    num_update_failures = 0

    for item in model_class.rate_limited_scan(_build_lba_filter_condition(attribute_names),
                                              read_capacity_to_consume_per_second=read_capacity_to_consume_per_second,
                                              allow_rate_limited_scan_without_consumed_capacity=unit_testing):
        actions = []
        condition = None
        for attr_name in attribute_names:
            if not hasattr(item, attr_name):
                raise ValueError('attribute {} does not exist on model'.format(attr_name))
            old_value = getattr(item, attr_name)
            if old_value is None:
                continue
            if not isinstance(old_value, bool):
                raise ValueError('attribute {} does not appear to be a boolean attribute'.format(attr_name))

            actions.append(getattr(model_class, attr_name).set(getattr(item, attr_name)))

            if condition is None:
                condition = Path(attr_name) == (1 if old_value else 0)
            else:
                condition = condition & Path(attr_name) == (1 if old_value else 0)

        if actions:
            if mock_conditional_update_failure:
                condition = condition & (Path('__bogus_mock_attribute') == 5)
            try:
                num_items_with_actions += 1
                item.update(actions=actions, condition=condition)
            except UpdateError as e:
                if isinstance(e.cause, ClientError):
                    code = e.cause.response['Error'].get('Code')
                    if code == 'ConditionalCheckFailedException':
                        log.warn('conditional update failed (concurrent writes?) for object: %s (you will need to re-run migration)', item)
                        num_update_failures += 1
                    else:
                        raise
                else:
                    raise
    log.info('finished migrating; %s items required updates, %s failed due to racing writes and require re-running migration',
             num_items_with_actions, num_update_failures)
    return num_items_with_actions, num_update_failures
