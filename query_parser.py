import traceback
import pandas as pd
import aws_s3
import datetime

# Global management table with all failed lines
manage_table = pd.DataFrame(columns=['filename', 'query_id', 'traceback', 'query'])
# Global check if any file failed before uploading management table (preventing uploading empty table)
any_file_failed = False
# Global date for dir name
date_today = str(datetime.date.today())


# Finding all query destinations for single query line
def find_query_destination(query):
    origin_query = query
    list_of_dicts_with_destinations = []
    multiple_from_or_join = False
    first_loop = True

    while True:
        dict_of_destinations = {"db": 'null', "schema": 'null', "table": 'null'}
        if not multiple_from_or_join:
            try:
                index_of_from = query.lower().index('from') + 5
            except ValueError:
                index_of_from = -1
            try:
                index_of_from_space = query.lower().index('from ') + 5
            except ValueError:
                index_of_from_space = -1
            try:
                index_of_join = query.lower().index('join') + 5
            except ValueError:
                index_of_join = -1
            if index_of_from != -1 and index_of_from_space != -1:
                if index_of_from > index_of_from_space:
                    index_of_from = index_of_from_space
            elif index_of_from == -1:
                index_of_from = index_of_from_space
            if index_of_from == index_of_join:
                # in case 'from ' and 'join' not in query, trying to find 'from' without space after
                try:
                    index_of_from = query.lower().index('from') + 5
                    if query[index_of_from] == ' ':
                        # in case after from there is new line and spaces before the relevant destination
                        index_of_first_char = query[index_of_from:].index(query[index_of_from:].replace(' ', '')[0])
                        index_of_from += index_of_first_char
                    elif query[index_of_from-1] == '\n':
                        if query[index_of_from] == '\n':
                            index_of_from = query.lower().index('from') + 6
                        else:
                            index_of_from = query.lower().index('from') + 5
                    elif query[index_of_from-1] == '\"':
                        index_of_from = query.lower().index('from') + 5
                    else:
                        # in case 'from' is part of the sentence starting query after 'from' and re-looping
                        query = query[index_of_from:]
                        continue
                except ValueError:
                    if index_of_from == index_of_join and first_loop:
                        # in case 'from' or 'join' not in query at all, finishing function
                        raise Exception('Failed to find FROM or JOIN in query')
                    else:
                        # in case no more 'from' or 'join' in query, finishing while loop
                        break
            if index_of_from != -1:
                if index_of_join != -1:
                    # in case both 'from' and 'join' in query
                    if index_of_from < index_of_join:
                        # in case 'from' before join
                        index_of_from_or_join = index_of_from
                    else:
                        # in case 'join' before 'from'
                        if query[index_of_join - 1] != '(':
                            index_of_from_or_join = index_of_join
                        else:
                            index_of_from_or_join = index_of_join - 1
                else:
                    # in case 'from' in query while 'join' is not
                    index_of_from_or_join = index_of_from
            else:
                # in case 'join' in query while 'from' is not
                if query[index_of_join-1] != '(':
                    index_of_from_or_join = index_of_join
                else:
                    index_of_from_or_join = index_of_join-1
            # starting query after 'from' or 'join'
            query = query[index_of_from_or_join:]
        else:
            multiple_from_or_join = False
        first_loop = False
        # temp_query for finding relevant query destinations
        temp_query = query
        if len(query.replace(' ', '')) == 0:
            break
        if query.replace(' ', '').replace('\n', '')[0] != '(' and query.replace(' ', '').replace('\n', '')[0] != '.':
            # in case it's not 'from ( select..... or join ( select.....'
            if not multiple_from_or_join:
                # finding end of relevant query destination
                try:
                    index_of_first_space = query.replace('\n', '').index(' ')
                    if index_of_first_space == 0:
                        while query.replace('\n', '').index(' ') == 0:
                            index_of_first_space = query.index(' ')+1
                            query = query[index_of_first_space:]
                        index_of_first_space = query.index(' ')
                except ValueError:
                    index_of_first_space = -1
                try:
                    index_of_scope = query.index('(')
                except ValueError:
                    index_of_scope = -1
                try:
                    index_of_break_line = query.index('\n')
                    if index_of_break_line == 0:
                        index_of_break_line = query[1:].index('\n')
                except ValueError:
                    index_of_break_line = -1
                if index_of_scope != -1 and index_of_first_space != -1 and (
                        index_of_scope - 1 == index_of_first_space or
                        index_of_first_space > index_of_scope):

                    # in case it's 'from #relevant_destinnation (....'
                    temp_query = query.replace(' ', '')
                    index_of_scope = temp_query.index('(')
                    temp_query = temp_query[:index_of_scope]
                    query = query[index_of_scope + 1:]

                else:
                    if index_of_first_space != -1:
                        if index_of_break_line != -1:
                            # in case both 'break line' and 'space' in query
                            if index_of_first_space < index_of_break_line:
                                # in case 'space' before 'break line'
                                temp_query = query[0:index_of_first_space]
                                query = query[index_of_first_space + 1:]
                            else:
                                # in case 'break line' before 'space'
                                temp_query = query[0:index_of_break_line]
                                query = query[index_of_break_line + 1:]
                        else:
                            # in case 'space' in query and 'break line' is not
                            temp_query = query[0:index_of_first_space]
                            query = query[index_of_first_space + 1:]
                    elif index_of_break_line != -1:
                        # in case 'break line' in query and 'space' is not
                        temp_query = query[:index_of_break_line]
                        query = query[index_of_break_line + 1:]
                    else:
                        # in case both of them not in query (relevant destination until end of query)
                        temp_query = query
            if len(temp_query) > 256:
                # in case something went bad and temp query super long that impossible
                continue
            if temp_query != '':
                if temp_query[-1] == ',':
                    # multiple from turn on
                    temp_query = temp_query[0:-1]
                    multiple_from_or_join = True
            number_of_dots = temp_query.count('.')
            if number_of_dots == 0:
                # only table name
                if 'as ' + temp_query not in origin_query and temp_query + ' as' not in origin_query:
                    # in case it's not temp name
                    dict_of_destinations["table"] = temp_query
            elif number_of_dots == 1:
                # schema name and table name
                dict_of_destinations["schema"] = temp_query[0:temp_query.index('.')]
                temp_query = temp_query[temp_query.index('.') + 1:]
                dict_of_destinations["table"] = temp_query
            else:
                # db name, schema name and table name
                dict_of_destinations["db"] = temp_query[0:temp_query.index('.')]
                temp_query = temp_query[temp_query.index('.') + 1:]
                dict_of_destinations["schema"] = temp_query[0:temp_query.index('.')]
                temp_query = temp_query[temp_query.index('.') + 1:]
                dict_of_destinations["table"] = temp_query
            if dict_of_destinations not in list_of_dicts_with_destinations and dict_of_destinations["table"] != 'null':
                # in case dictionary isn't already in list and not without table name
                list_of_dicts_with_destinations.append(dict_of_destinations)
        else:
            # in case it's 'from ( select..... or join ( select.....', re-looping
            continue
    return list_of_dicts_with_destinations


def parsing(source):
    global manage_table
    global any_file_failed
    file_failed = False
    index_of_file_name = str(source).index('query_text')
    file_name = str(source)[index_of_file_name:]
    temp_df = aws_s3.read_to_csv(source, False)
    df = pd.DataFrame(columns=['file_name', 'query_id', 'user_name', 'role_name', 'db_name', 'schema_name',
                               'table_name', 'query_text'])

    for index, rows in temp_df.iterrows():
        try:
            list_of_dicts_with_destinations = find_query_destination(rows[5])
        except Exception:
            # in case single query failed
            trace = traceback.format_exc()
            manage_table = manage_table.append(pd.Series([file_name, rows[0], trace, rows[5]],
                                                         index=manage_table.columns), ignore_index=True)
            file_failed = True
            any_file_failed = True
            continue
        first_loop = True
        for dicts in list_of_dicts_with_destinations:
            # checking all parsed destinations from single query and saving in DataFrame
            if dicts["table"] != 'null' and dicts["table"] != '':
                if dicts["db"] == "null" and rows[3] != '\\N':
                    db_name = rows[3]
                else:
                    db_name = dicts["db"]
                if '\"' in db_name:
                    db_name = db_name.replace('\"', '')
                if dicts["schema"] == "null" and rows[4] != '\\N':
                    schema_name = rows[4]
                else:
                    schema_name = dicts["schema"]
                if '\"' in schema_name:
                    schema_name = schema_name.replace('\"', '')
                table = dicts["table"]
                if ')' in dicts["table"]:
                    table = dicts["table"].replace(')', '')
                if '\"' in dicts["table"]:
                    table = dicts["table"].replace('\"', '')
                    if "#" in dicts["table"]:
                        table = table.replace('#', '')
                if '\r' in table:
                    table = table.replace('\r', '')
                if first_loop:
                    # with original query
                    df = df.append(
                        pd.Series([file_name, rows[0], rows[1], rows[2], db_name.upper(), schema_name.upper(),
                                   table.upper(), rows[5]], index=df.columns), ignore_index=True)
                    first_loop = False
                else:
                    # without original query if it's same query (reduce memory)
                    df = df.append(
                        pd.Series([file_name, rows[0], rows[1], rows[2], db_name.upper(), schema_name.upper(),
                                   table.upper(), ''], index=df.columns), ignore_index=True)
                    first_loop = False
    # uploading parsed csv file to s3 (target dir)
    aws_s3.upload_file(df, aws_s3.target_path_name + date_today + "/" + file_name[:-4] + '_target.csv')
    if file_failed:
        # uploading source csv to s3 (failed dir) if failed
        aws_s3.move_file(source, aws_s3.failed_path_name + date_today + "/" + file_name)
    else:
        # uploading source csv to s3 (processed dir) if parsed successfully
        aws_s3.move_file(source, aws_s3.processed_path_name + date_today + "/" + file_name)


if __name__ == '__main__':
    for f in aws_s3.get_list_of_files(aws_s3.source_path_name):
        # runs on all files in s3 source dir
        if f.key != aws_s3.prefix_name + aws_s3.source_path_name:
            # != due to f.key returns csv files and dir path (preventing sending dir path to parsing func as csv file)
            parsing(f.key)
    if any_file_failed:
        # uploading management table to s3 (management dir) if any file failed
        aws_s3.upload_file(manage_table, aws_s3.management_path_name + date_today + '/management_table.csv')