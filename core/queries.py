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

async def get_last_launch(cursor, exchange_1: str, exchange_2: str, coin: str, updated_flag: int, limit: bool) -> dict:
    sql = f"""
    select 
        *
    from 
        bot_launches
    where 
        exchange_1 in ('{exchange_1}', '{exchange_2}') and 
        exchange_2 in ('{exchange_1}', '{exchange_2}') and
        coin = '{coin}' and 
        updated_flag = {updated_flag}
    order by 
    	datetime desc
    {"limit 1" if limit else ""}
    """

    print(sql)
    # return await cursor.fetch(sql)


async def get_last_balance_jumps(cursor):
    sql = """
    select 
        total_balance,
        TO_CHAR(TO_TIMESTAMP(ts  / 1000), 'YYYY-MM-DD HH24:MI:SS') as date_utc
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
