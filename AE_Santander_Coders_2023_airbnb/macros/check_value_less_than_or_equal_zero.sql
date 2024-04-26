{% test check_value_less_than_or_equal_zero(model, column_name) %}
SELECT * FROM {{ model }} 
   WHERE {{ column_name}} <= 0
{% endtest %}