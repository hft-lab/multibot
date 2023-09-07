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


async def get_last_deals(cursor):
    sql = f"""
        select 
            *
        from 
            arbitrage_possibilities 
        order by
            datetime desc
        limit 5
    """

    return await cursor.fetch(sql)


async def get_last_launch(cursor, exchange_1: str, exchange_2: str, coin: str, update_flag=0) -> list:
    sql = f"""
    select 
        *
    from 
        bot_launches
    where 
        exchange_1 in ('{exchange_1.upper()}', '{exchange_2.upper()}') and 
        exchange_2 in ('{exchange_1.upper()}', '{exchange_2.upper()}') and
        coin = '{coin.upper()}' and 
        updated_flag = {update_flag}
    order by 
        datetime desc
    """

    if res := await cursor.fetch(sql):
        return [dict(x) for x in res]
    return []


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
    if res := await cursor.fetchrow(sql):
        return res['total_balance'], res['date_utc']
