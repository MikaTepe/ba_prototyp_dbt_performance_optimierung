{% macro silvershot_strategy_dispatch(name) %}

    {% set original_name = name %}
    {% if '.' in name %}
        {% set package_name, name = name.split(".", 1) %}
    {% else %}
        {% set package_name = none %}
    {% endif %}

    {% if package_name is none %}
        {% set package_context = context %}
    {% elif package_name in context %}
        {% set package_context = context[package_name] %}
    {% else %}
        {% set error_msg %}
            Could not find package '{{package_name}}', called with '{{original_name}}'
        {% endset %}
        {{ exceptions.raise_compiler_error(error_msg | trim) }}
    {% endif %}

    {%- set search_name = 'silvershot_' ~ name ~ '_strategy' -%}

    {% if search_name not in package_context %}
        {% set error_msg %}
            The specified strategy macro '{{name}}' was not found in package '{{ package_name }}'
        {% endset %}
        {{ exceptions.raise_compiler_error(error_msg | trim) }}
    {% endif %}
    {{ return(package_context[search_name]) }}

{% endmacro %}





