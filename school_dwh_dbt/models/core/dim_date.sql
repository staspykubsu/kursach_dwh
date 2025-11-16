-- DONE

{% if not is_incremental() %}
    {{
        config(
            materialized='table',
            order_by='date_key',
            engine='MergeTree()'
        )
    }}

    with date_series as (
        select 
            toDate('2020-01-01') + number as date_actual
        from 
            numbers(toUInt64(toDate('2030-12-31') - toDate('2020-01-01') + 1))
    )

    select 
        toYYYYMMDD(date_actual) as date_key,
        date_actual,
        toYear(date_actual) as year,
        toQuarter(date_actual) as quarter,
        toMonth(date_actual) as month,
        toDayOfMonth(date_actual) as day,
        toDayOfWeek(date_actual) as day_of_week,
        case toMonth(date_actual)
            when 1 then 'Январь'
            when 2 then 'Февраль'
            when 3 then 'Март'
            when 4 then 'Апрель'
            when 5 then 'Май'
            when 6 then 'Июнь'
            when 7 then 'Июль'
            when 8 then 'Август'
            when 9 then 'Сентябрь'
            when 10 then 'Октябрь'
            when 11 then 'Ноябрь'
            when 12 then 'Декабрь'
        end as month_name,
        case toDayOfWeek(date_actual)
            when 1 then 'Понедельник'
            when 2 then 'Вторник'
            when 3 then 'Среда'
            when 4 then 'Четверг'
            when 5 then 'Пятница'
            when 6 then 'Суббота'
            when 7 then 'Воскресенье'
        end as day_name_ru,
        if(toDayOfWeek(date_actual) in (6, 7), 1, 0) as is_weekend,
        case 
            when toMonth(date_actual) between 3 and 5 then 'Весна'
            when toMonth(date_actual) between 6 and 8 then 'Лето'
            when toMonth(date_actual) between 9 and 11 then 'Осень'
            else 'Зима'
        end as season
    from date_series
{% else %}
    -- При инкрементальных запусках просто выбираем из существующей таблицы
    select * from {{ this }}
{% endif %}