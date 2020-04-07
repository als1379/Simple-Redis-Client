import asyncio


class RedisClient:

    async def connect(self, host, port):
        self.r , self.w = await asyncio.open_connection(host, port)

    async def send(self, *args):
        resp_args = "".join([f"${len(x)}\r\n{x}\r\n" for x in args])
        self.w.write(f"*{len(args)}\r\n{resp_args}".encode())
        await self.w.drain()
        return await self._read_reply()

    async def _read_reply(self):
        tag = await self.r.read(1)

        if tag == b'$':
            length = b''
            ch = b''
            while ch != b'\n':
                ch = await self.r.read(1)
                length += ch
            length = int(length[:-1]) + 2
            result = await self.r.read(length)
            return result[:-2].decode()

        if tag == b':':
            result = b''
            ch = b''
            while ch != b'\n':
                ch = await  self.r.read(1)
                result += ch
            return int(result[:-1].decode())

        if tag == b'-':
            result = b''
            ch = b''
            while ch != b'\n':
                ch = await  self.r.read(1)
                result += ch
            raise Exception(result[:-1].decode())

        if tag == b'+':
            result = b''
            ch = b''
            while ch != b'\n':
                ch = await  self.r.read(1)
                result += ch
            return result[:-1].decode()

        if tag == b'*':
            len = int(await self.r.read(1))
            enter = await self.r.read(2)
            result = []
            for _ in range(len):
                dollar = await self.r.read(1)
                key_len = int(await self.r.read(1))
                enter = await self.r.read(2)
                key = await self.r.read(key_len)
                result.append(key.decode())
                enter = await self.r.read(2)

            return result
        else:
            msg = await self.r.read(100)
            raise Exception(f"Unkown tag: {tag}, msg: {msg}")


async def start():
    client = RedisClient()
    await client.connect("localhost", 6379)
    query = ""
    while query != "quit":
        query = input("enter query: ").split()
        if query[0] == "quit":
            break
        print(await client.send(*query))

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(start())
