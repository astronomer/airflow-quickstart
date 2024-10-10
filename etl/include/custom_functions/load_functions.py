def get_sql_query(df, table_name):
    sql_text = []
    for index, row in df.iterrows():       
        sql_text.append(
                f"INSERT INTO {table_name} ("+ str(", ".join(df.columns))+ ") VALUES "+ str(tuple(row.values))
            )        
    return sql_text
