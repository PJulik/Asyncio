import asyncio
import aiohttp
import datetime
from more_itertools import chunked

from models import Base, Session, SwapiPeople, engine

MAX_CHUNK = 5

async def get_person(person_id, session):
    http_response = await session.get(f'https://swapi.py4e.com/api/people/{person_id}')
    json_data = await http_response.json()
    return json_data

async def get_info(urls):
    async with aiohttp.ClientSession() as session:
        htmls = await fetch_all(session, urls)
        return htmls

async def fetch(session, url):
    async with session.get(url) as response:
        if response.status != 200:
            response.raise_for_status()
        return await response.json()

async def fetch_all(session, urls):
    tasks = []
    for url in urls:
        task = asyncio.create_task(fetch(session, url))
        tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results

async def insert_people(results):
    async with Session() as session:
        people_list = []
        for person_json in results:
            if person_json.get('url'):
                films = await get_info(person_json.get('films'))
                homeworld = await get_info([person_json.get('homeworld')])
                species = await get_info(person_json.get('species'))
                starships = await get_info(person_json.get('starships'))
                vehicles = await get_info(person_json.get('vehicles'))

                person = SwapiPeople(
                    id=int(person_json.get("url")[34:-1]),
                    birth_year=person_json.get("birth_year"),
                    eye_color=person_json.get("eye_color"),
                    films=", ".join([film.get("title") for film in films]),
                    gender=person_json.get("gender"),
                    hair_color=person_json.get("hair_color"),
                    height=person_json.get("height"),
                    homeworld=homeworld[0].get("name"),
                    mass=person_json.get("mass"),
                    name=person_json.get("name"),
                    skin_color=person_json.get("skin_color"),
                    species=", ".join([specie.get("name") for specie in species]),
                    starships=", ".join(
                        [starship.get("name") for starship in starships]
                    ),
                    vehicles=", ".join([vehicle.get("name") for vehicle in vehicles]),
                )
                people_list.append(person)

        session.add_all(people_list)
        await session.commit()

async def main():
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.drop_all)
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)
    async with aiohttp.ClientSession() as client:
        for ids_chunk in chunked(range(1, 84), MAX_CHUNK):
            coros = [get_person(client, i) for i in ids_chunk]
            results = await asyncio.gather(*coros)
            insert_to_db_coro = insert_people(results)
            asyncio.create_task(insert_to_db_coro)
    current_task = asyncio.current_task()
    tasks_to_await = asyncio.all_tasks() - {
        current_task,
    }
    for task in tasks_to_await:
        await task

if __name__ == '__main__':
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)
