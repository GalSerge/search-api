query_select_on_time = 'SELECT {table_id}, {field_id}, {lang}, ' \
                           '{title}, {content} ' \
                           'FROM {table_name} ' \
                           'WHERE {field_update} > NOW() - INTERVAL {every} DAY AND ' \
                           '{min_id} <= id AND id < {max_id} '

query_select = 'SELECT {table_id}, {field_id}, {lang}, ' \
               '{title}, {content} ' \
               'FROM {table_name} ' \
               'WHERE {min_id} <= id AND id < {max_id} '

query_num_rows = 'SELECT `id` FROM {table_name} ORDER BY `id` DESC LIMIT 1'

query_select_from_task = 'SELECT {table_id}, {table_name}.{field_id}, {lang}, ' \
                         '{title}, {content} {optional}  ' \
                         'FROM {table_name} ' \
                         'INNER JOIN ' \
                         '(SELECT * FROM {tasks_table} ' \
                         'WHERE table_id={table_id} AND ' \
                         '{min_id} <= id AND id < {max_id} AND ' \
                         'act=\'{act}\') AS `tasks_table_` ' \
                         'ON tasks_table_.field_id={table_name}.{field_id} AND ' \
                         'tasks_table_.lang_id={table_name}.{field_lang}'

query_select_task = 'SELECT `table_id`, `field_id`, `lang_id`, `act`, `coef` FROM {tasks_table} ' \
                    'WHERE {min_id} <= id AND id < {max_id} AND ' \
                    'act=\'{act}\' '

query_delete_task = 'DELETE FROM {tasks_table} WHERE `act`={act}'
