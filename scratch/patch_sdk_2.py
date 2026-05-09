import re

def patch_file(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    # We want to replace:
    # {whitespace}await self._rate_limiter.acquire()
    # {whitespace}{var_name} = await loop.run_in_executor(
    # {whitespace}None, {lambda_expr}
    # {whitespace})
    # OR
    # {whitespace}None,
    # {whitespace}{lambda_expr}
    # {whitespace}),
    # {whitespace})
    
    # A robust regex for this file: Find `await self._rate_limiter.acquire()`
    # Then `\s+([a-zA-Z0-9_]+) = await loop\.run_in_executor\(\s*None,\s*(lambda.*?)\)`
    # BUT lambda.*? might not capture multiline lambda correctly if we use non-greedy.
    
    # Let's just do targeted line replacements because we know exactly where they are.
    
    # Snipe Maker (around line 420):
    content = re.sub(
        r'(\s+)await self\._rate_limiter\.acquire\(\)\n\s+result = await loop\.run_in_executor\(\n\s+None, lambda: self\._client\.create_and_post_order\(order_args, options\)\n\s+\)',
        r'\1result = await self._sdk_call(\n\1    lambda: self._client.create_and_post_order(order_args, options)\n\1)',
        content
    )

    # sell_with_retry (around line 873):
    content = re.sub(
        r'(\s+)await self\._rate_limiter\.acquire\(\)\n\s+result = await loop\.run_in_executor\(\n\s+None,\n\s+lambda: self\._client\.create_and_post_order\(\n\s+order_args, PartialCreateOrderOptions\(\)\n\s+\),\n\s+\)',
        r'\1result = await self._sdk_call(\n\1    lambda: self._client.create_and_post_order(\n\1        order_args, PartialCreateOrderOptions()\n\1    )\n\1)',
        content
    )

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
        
    print("Success")

if __name__ == "__main__":
    patch_file(r"c:\Users\yuval\polymarket_sniper\execution\order_manager.py")
