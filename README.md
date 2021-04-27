# module-3-airflow
## Тесты для таблиц
### Для проверки количества строк (полнота данных)
batch.expect_table_row_count_to_equal(value=N), где N - количество строк в каждой из таблиц
### Для проверки количества колонок (полнота данных)
batch.expect_table_column_count_to_equal(value=M), где M - количество колонок в каждой из таблиц
### Проверка совпадения названий столбцов с указааным списком
batch.expect_table_columns_to_match_ordered_list(column_list=[LIST]), LIST-список названий столбцов для каждой таблицы, например('user_id', 'pay_doc_type', 'pay_doc_num', 'account', 'phone', 'billing_period', 'pay_date', 'sum')
## Тесты для колонок
### Проверка отсутствия пустых значений в колонке
batch.expect_column_values_to_not_be_null(column=S), где S - название колонки
### Проверка, что число уникальных значений в колонке лежит в диапазоне
batch.expect_column_unique_value_count_to_be_between(column=S, min_value=i, max_value=j), где S - название колонки, i -минимальное значение диапазона, j - максимальное значение диапазона
### Проверка, что уникальные значения в колонке соответсвуют списку (подходит для колонок, в которых небольшое число уникальных значений)
batch.expect_column_distinct_values_to_equal_set(column='pay_doc_type', value_set=['MASTER', 'MIR', 'VISA'])
### Проверка, что значения в колонке лежат в диапазоне (для дат)
batch.expect_column_values_to_be_between(column='pay_date', max_value='2020-12-31 00:00:00', min_value='2013-01-01 00:00:00', parse_strings_as_datetimes=True)
### Проверка, максимальное и минимальное значение в колонке лежат в диапазоне (для чисел)
batch.expect_column_min_to_be_between(column='user_id', max_value=j, min_value=i)
batch.expect_column_max_to_be_between(column='user_id', max_value=j, min_value=i), где  i -минимальное значение диапазона, j - максимальное значение
### Проверка, среднее и медиана лежат в диапазоне (для колонок sum)
batch.expect_column_mean_to_be_between(column='sum', max_value=j, min_value=i)
batch.expect_column_median_to_be_between(column='sum', max_value=j, min_value=i), где  i -минимальное значение диапазона, j - максимальное значение
