async def get_total_balance(cursor, asc_desc, exchanges_len):
    sql = f"""
    select 
        sum(d.total_balance) as bal
    from 
        (select 
            distinct on (bc.exchange_name) bc.exchange_name, ts, total_balance
        from 
            balance_check bc
        where 
            bc.ts > extract(epoch from current_date at time zone 'UTC') * 1000
        order by 
            exchange_name, ts {asc_desc}
        limit 
            {exchanges_len}) d  
    """

    res = await cursor.fetchrow(sql)
    return res['bal']


async def get_last_balance_jumps(cursor):
    sql = """
    select 
        *
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
        return res['total_balance']
