async def get_total_balance(cursor, asc_desc):
    sql = f"""
        select 
            *
        from 
            balance_check bc 
        order by
            ts {asc_desc} 
    """

    return await cursor.fetch(sql)


async def get_last_balance_jumps(cursor):
    sql = """
    select 
        total_balance,
        TO_CHAR(TO_TIMESTAMP(ts  / 1000), 'DD/MM/YYYY HH24:MI:SS') as date_utc
    from 
        balance_jumps
    where 
        ts > extract(epoch from current_date at time zone 'UTC') * 1000
    order by 
        ts desc
    limit 
        1
    """
    if res:= await cursor.fetchrow(sql):
        return res['total_balance'], res['date_utc']
