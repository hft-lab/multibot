import configparser
import asyncio
import asyncpg

config = configparser.ConfigParser()
config.read("config.ini", "utf-8")

async def setup_postgres() -> None:
    # print(config.POSTGRES)
    postgres = config['POSTGRES']
    print(postgres['NAME'],postgres['USER'],postgres['PASSWORD'],postgres['HOST'],postgres['PORT'])
    db = await asyncpg.create_pool(database=postgres['NAME'],
                                        user=postgres['USER'],
                                        password=postgres['PASSWORD'],
                                        host=postgres['HOST'],
                                        port=postgres['PORT'])
    print('hola')
async def main():
    await setup_postgres()
    print('Hi')

if __name__ == '__main__':
    asyncio.run(main())