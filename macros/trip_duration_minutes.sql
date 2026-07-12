{#
    Returns trip duration in whole minutes between two timestamps.
    Kept as a macro to demonstrate reusable Jinja logic and to keep
    duration arithmetic consistent across models.
#}
{% macro trip_duration_minutes(start_col, end_col) %}
    cast(date_diff('minute', {{ start_col }}, {{ end_col }}) as integer)
{% endmacro %}
