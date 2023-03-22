select = 'SELECT {table_id}, {field_id}, {field_lang}, {field_active} ' \
         '{title} {content} {optional} ' \
         'FROM {table_name} ' \

select_task = 'SELECT {table_id}, {table_name}.{field_id}, {table_name}.{field_lang}, _task_table.act, _task_table.coef {title} {content} {optional} ' \
              'FROM {table_name} ' \
              'INNER JOIN ' \
              '(SELECT * FROM {task_table} ' \
              'WHERE table_id={table_id}) AS _task_table ' \
              'ON _task_table.field_id={table_name}.{field_id} AND ' \
              '_task_table.lang_id={table_name}.{field_lang}'

num_rows = 'SELECT COUNT(*) FROM {table_name}'

where_timestamp = ' {field_timestamp} >= (NOW() - INTERVAL {every_day} DAY) '

where_active = ' {field_active} = {active_value} '

limit = ' LIMIT {batch_size} OFFSET {start} '
