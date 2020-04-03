import asyncio


class RedisClient:

    async def connect(self, host, port):
        self.r , self.w = await asyncio.open_connection(host, port)

    async def set(self, key, value):
        self.w.write(f"SET {key} {value} \r\n".encode())
        await self.w.drain()
        return await self._read_reply()

    async def get(self, key):
        self.w.write(f"GET {key} \r\n".encode())
        await self.w.drain()
        return await self._read_reply()

    async def _read_reply(self):
        tag = await self.r.read(1)

        if tag == b'+':
            result = b''
            ch = b''
            while ch != b'\n':
                ch = await  self.r.read(1)
                result += ch
            return result[:-1].decode()
        else:
            msg = await self.r.read(100)
            raise Exception(f"Unkown tag: {tag}, msg: {msg}")


async def start():
    client = RedisClient()
    await client.connect("localhost", 6379)
    print(await client.set("ali", "fuck"))
    print(await client.get("ali"))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start())
