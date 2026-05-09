import os

def patch_file(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # Add the method
    sdk_call_code = """
    async def _sdk_call(self, func):
        \"\"\"Execute a py-clob-client lambda function with automatic retry for 425 'service not ready'.\"\"\"
        loop = __import__('asyncio').get_event_loop()
        max_attempts = 4
        for attempt in range(1, max_attempts + 1):
            await self._rate_limiter.acquire()
            try:
                return await loop.run_in_executor(None, func)
            except Exception as exc:
                exc_str = str(exc).lower()
                if "425" in exc_str or "service not ready" in exc_str:
                    if attempt < max_attempts:
                        delay = 0.5 * (2 ** (attempt - 1))
                        import logging
                        logging.getLogger(__name__).warning("API 425 'service not ready' — waiting %.2fs and retrying (attempt %d/%d)", delay, attempt, max_attempts)
                        await __import__('asyncio').sleep(delay)
                        continue
                raise

    async def cancel_all(self) -> None:"""

    content = content.replace("    async def cancel_all(self) -> None:", sdk_call_code)

    replaces = [
        (
            """            await self._rate_limiter.acquire()
            result = await loop.run_in_executor(
                None, lambda: self._client.create_and_post_order(order_args, options)
            )""",
            """            result = await self._sdk_call(
                lambda: self._client.create_and_post_order(order_args, options)
            )"""
        ),
        (
            """            await self._rate_limiter.acquire()
            signed_order = await loop.run_in_executor(
                None, lambda: self._client.create_order(order_args, options)
            )""",
            """            signed_order = await self._sdk_call(
                lambda: self._client.create_order(order_args, options)
            )"""
        ),
        (
            """            await self._rate_limiter.acquire()
            result = await loop.run_in_executor(
                None, lambda: self._client.post_order(signed_order, OrderType.FAK)
            )""",
            """            result = await self._sdk_call(
                lambda: self._client.post_order(signed_order, OrderType.FAK)
            )"""
        ),
        (
            """            await self._rate_limiter.acquire()
            result = await loop.run_in_executor(
                None,
                lambda: self._client.create_and_post_order(
                    order_args, PartialCreateOrderOptions()
                ),
            )""",
            """            result = await self._sdk_call(
                lambda: self._client.create_and_post_order(
                    order_args, PartialCreateOrderOptions()
                )
            )"""
        ),
        (
            """            await self._rate_limiter.acquire()
            signed = await loop.run_in_executor(
                None, lambda: self._client.create_order(order_args, options)
            )""",
            """            signed = await self._sdk_call(
                lambda: self._client.create_order(order_args, options)
            )"""
        ),
        (
            """            await self._rate_limiter.acquire()
            result = await loop.run_in_executor(
                None, lambda: self._client.post_order(signed, OrderType.FAK)
            )""",
            """            result = await self._sdk_call(
                lambda: self._client.post_order(signed, OrderType.FAK)
            )"""
        ),
        (
            """            await self._rate_limiter.acquire()
            signed_order = await loop.run_in_executor(
                None, lambda: self._client.create_market_order(market_args, options)
            )""",
            """            signed_order = await self._sdk_call(
                lambda: self._client.create_market_order(market_args, options)
            )"""
        ),
        (
            """            await self._rate_limiter.acquire()
            result = await loop.run_in_executor(
                None, lambda: self._client.post_order(signed_order, OrderType.FOK)
            )""",
            """            result = await self._sdk_call(
                lambda: self._client.post_order(signed_order, OrderType.FOK)
            )"""
        )
    ]

    for old, new in replaces:
        content = content.replace(old, new)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    print("Success")

if __name__ == "__main__":
    patch_file(r"c:\Users\yuval\polymarket_sniper\execution\order_manager.py")
