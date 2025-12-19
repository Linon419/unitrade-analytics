import asyncio
import aiohttp

async def check_ban():
    async with aiohttp.ClientSession() as s:
        async with s.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as r:
            print(f"Status: {r.status}")
            if r.status == 418:
                # Check Retry-After header
                retry_after = r.headers.get("Retry-After", "unknown")
                print(f"IP BANNED - Retry-After: {retry_after} seconds")
            elif r.status == 429:
                retry_after = r.headers.get("Retry-After", "unknown")
                print(f"Rate limited - Retry-After: {retry_after} seconds")
            elif r.status == 200:
                print("API OK - IP NOT banned!")
            else:
                text = await r.text()
                print(f"Response: {text[:200]}")

asyncio.run(check_ban())
