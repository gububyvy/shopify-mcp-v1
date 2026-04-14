#!/usr/bin/env python3
"""
Shopify MCP Server — Full Admin API access via FastMCP.
Provides tools for managing products, orders, customers, collections,
inventory, fulfillments, navigation menus, pages, blogs/articles,
policies, markets, redirects, and theme assets through the Shopify
Admin REST + GraphQL API.

Token Management:
  - Uses client_credentials grant to auto-generate and refresh tokens
  - Set SHOPIFY_CLIENT_ID + SHOPIFY_CLIENT_SECRET (recommended for OAuth apps)
  - Falls back to static SHOPIFY_ACCESS_TOKEN if client credentials not set
"""
import json
import os
import logging
import time
import asyncio
from typing import Optional, List, Dict, Any
from enum import Enum
import httpx
from pydantic import BaseModel, Field, ConfigDict, field_validator
from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SHOPIFY_STORE        = os.environ.get("SHOPIFY_STORE", "")           # e.g. "my-store"
SHOPIFY_TOKEN        = os.environ.get("SHOPIFY_ACCESS_TOKEN", "")    # Static token (shpat_...)
SHOPIFY_CLIENT_ID    = os.environ.get("SHOPIFY_CLIENT_ID", "")
SHOPIFY_CLIENT_SECRET = os.environ.get("SHOPIFY_CLIENT_SECRET", "")
API_VERSION          = os.environ.get("SHOPIFY_API_VERSION", "2024-10")

# Refresh buffer: refresh token 30 minutes before expiry (only used with OAuth)
TOKEN_REFRESH_BUFFER = int(os.environ.get("TOKEN_REFRESH_BUFFER", "1800"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("shopify_mcp")

PORT          = int(os.environ.get("PORT", "8000"))
MCP_TRANSPORT = os.environ.get("MCP_TRANSPORT", "streamable-http")

mcp = FastMCP(f"shopify_mcp_{SHOPIFY_STORE}", host="0.0.0.0", port=PORT, json_response=True)


# ---------------------------------------------------------------------------
# Token Manager — handles automatic token lifecycle
# ---------------------------------------------------------------------------

class TokenManager:
    """
    Manages Shopify Admin API access tokens.

    Two modes:
      1. Static token  — set SHOPIFY_ACCESS_TOKEN (recommended for Custom Apps)
      2. OAuth / client_credentials — set SHOPIFY_CLIENT_ID + SHOPIFY_CLIENT_SECRET
         Enables auto-refresh before expiry and retry on 401.
    """

    def __init__(
        self,
        store: str,
        client_id: str,
        client_secret: str,
        static_token: str = "",
        refresh_buffer: int = 1800,
    ):
        self._store         = store
        self._client_id     = client_id
        self._client_secret = client_secret
        self._static_token  = static_token
        self._refresh_buffer = refresh_buffer

        self._access_token: str   = ""
        self._expires_at: float   = 0.0
        self._lock = asyncio.Lock()

        self._use_client_credentials = bool(client_id and client_secret)

        if self._use_client_credentials:
            logger.info("Token mode: client_credentials (auto-refresh enabled)")
        elif static_token:
            logger.info("Token mode: static SHOPIFY_ACCESS_TOKEN (no auto-refresh)")
            self._access_token = static_token
            self._expires_at   = float("inf")
        else:
            logger.warning(
                "No credentials configured. Set SHOPIFY_ACCESS_TOKEN or "
                "SHOPIFY_CLIENT_ID + SHOPIFY_CLIENT_SECRET."
            )

    @property
    def is_expired(self) -> bool:
        if not self._access_token:
            return True
        return time.time() >= (self._expires_at - self._refresh_buffer)

    async def get_token(self) -> str:
        if not self.is_expired:
            return self._access_token

        async with self._lock:
            if not self.is_expired:
                return self._access_token

            if self._use_client_credentials:
                await self._refresh_token()
            elif not self._access_token:
                raise RuntimeError(
                    "No valid token available. "
                    "Set SHOPIFY_ACCESS_TOKEN in your environment variables."
                )

        return self._access_token

    async def force_refresh(self) -> str:
        if not self._use_client_credentials:
            raise RuntimeError(
                "Cannot refresh — using a static token. "
                "Set SHOPIFY_CLIENT_ID + SHOPIFY_CLIENT_SECRET to enable auto-refresh."
            )
        async with self._lock:
            await self._refresh_token()
        return self._access_token

    async def _refresh_token(self) -> None:
        url = f"https://{self._store}.myshopify.com/admin/oauth/access_token"
        logger.info("Refreshing Shopify access token via client_credentials grant...")

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                url,
                data={
                    "grant_type":    "client_credentials",
                    "client_id":     self._client_id,
                    "client_secret": self._client_secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=15.0,
            )

            if resp.status_code != 200:
                logger.error(f"Token refresh failed ({resp.status_code}): {resp.text[:500]}")
                raise RuntimeError(
                    f"Token refresh failed ({resp.status_code}). "
                    "Check SHOPIFY_CLIENT_ID and SHOPIFY_CLIENT_SECRET."
                )

            data               = resp.json()
            self._access_token = data["access_token"]
            expires_in         = data.get("expires_in", 86399)
            self._expires_at   = time.time() + expires_in

            scope         = data.get("scope", "")
            scope_preview = scope[:80] + "..." if len(scope) > 80 else scope
            logger.info(
                f"Token refreshed. Expires in {expires_in}s "
                f"({expires_in // 3600}h {(expires_in % 3600) // 60}m). "
                f"Scopes: {scope_preview}"
            )


# Global token manager
token_manager = TokenManager(
    store=SHOPIFY_STORE,
    client_id=SHOPIFY_CLIENT_ID,
    client_secret=SHOPIFY_CLIENT_SECRET,
    static_token=SHOPIFY_TOKEN,
    refresh_buffer=TOKEN_REFRESH_BUFFER,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _base_url() -> str:
    return f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/{API_VERSION}"


async def _headers() -> dict:
    token = await token_manager.get_token()
    return {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
    }


async def _request(
    method: str,
    path: str,
    params: Optional[dict] = None,
    body:   Optional[dict] = None,
    _retried: bool = False,
) -> dict:
    """Central HTTP helper — every API call flows through here.
    Auto-retries once on 401 when using OAuth credentials.
    """
    if not SHOPIFY_STORE:
        raise RuntimeError(
            "Missing SHOPIFY_STORE environment variable. "
            "Set it before starting the server."
        )

    url     = f"{_base_url()}/{path}"
    headers = await _headers()

    async with httpx.AsyncClient() as client:
        resp = await client.request(
            method, url,
            headers=headers,
            params=params,
            json=body,
            timeout=30.0,
        )

        if resp.status_code == 401 and not _retried and token_manager._use_client_credentials:
            logger.warning("Got 401 — refreshing token and retrying...")
            await token_manager.force_refresh()
            return await _request(method, path, params=params, body=body, _retried=True)

        resp.raise_for_status()
        if resp.status_code == 204:
            return {}
        return resp.json()


async def _graphql(
    query: str,
    variables: Optional[dict] = None,
    _retried: bool = False,
) -> dict:
    """GraphQL helper — for APIs only available via Shopify GraphQL Admin API (e.g. menus)."""
    if not SHOPIFY_STORE:
        raise RuntimeError("Missing SHOPIFY_STORE environment variable.")

    url = f"{_base_url()}/graphql.json"
    headers = await _headers()
    payload: Dict[str, Any] = {"query": query}
    if variables:
        payload["variables"] = variables

    async with httpx.AsyncClient() as client:
        resp = await client.post(url, headers=headers, json=payload, timeout=30.0)

        if resp.status_code == 401 and not _retried and token_manager._use_client_credentials:
            logger.warning("GraphQL 401 — refreshing token and retrying...")
            await token_manager.force_refresh()
            return await _graphql(query, variables=variables, _retried=True)

        resp.raise_for_status()
        data = resp.json()

        if "errors" in data:
            raise RuntimeError(f"GraphQL errors: {json.dumps(data['errors'])}")

        return data.get("data", data)


def _error(e: Exception) -> str:
    if isinstance(e, httpx.HTTPStatusError):
        status = e.response.status_code
        try:
            detail = e.response.json()
        except Exception:
            detail = e.response.text[:500]
        messages = {
            401: "Authentication failed — check your SHOPIFY_ACCESS_TOKEN (should start with shpat_).",
            403: "Permission denied — your token may be missing required API scopes.",
            404: "Resource not found — double-check the ID.",
            422: f"Validation error: {json.dumps(detail)}",
            429: "Rate-limited — wait a moment and retry.",
        }
        return messages.get(status, f"Shopify API error {status}: {json.dumps(detail)}")
    if isinstance(e, httpx.TimeoutException):
        return "Request timed out — try again."
    if isinstance(e, RuntimeError):
        return str(e)
    return f"Unexpected error: {type(e).__name__}: {e}"


def _fmt(data: Any) -> str:
    return json.dumps(data, indent=2, default=str)


# ═══════════════════════════════════════════════════════════════════════════
# PRODUCTS
# ═══════════════════════════════════════════════════════════════════════════

class ListProductsInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    limit:          Optional[int]  = Field(default=50, ge=1, le=250, description="Max products to return (1-250)")
    status:         Optional[str]  = Field(default=None, description="Filter by status: active, archived, draft")
    product_type:   Optional[str]  = Field(default=None, description="Filter by product type")
    vendor:         Optional[str]  = Field(default=None, description="Filter by vendor name")
    collection_id:  Optional[int]  = Field(default=None, description="Filter by collection ID")
    since_id:       Optional[int]  = Field(default=None, description="Pagination: return products after this ID")
    fields:         Optional[str]  = Field(default=None, description="Comma-separated fields to include")


@mcp.tool(
    name="shopify_list_products",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_list_products(params: ListProductsInput) -> str:
    """List products from the Shopify store with optional filters."""
    try:
        p: Dict[str, Any] = {"limit": params.limit}
        for field in ["status", "product_type", "vendor", "collection_id", "since_id", "fields"]:
            val = getattr(params, field)
            if val is not None:
                p[field] = val
        data     = await _request("GET", "products.json", params=p)
        products = data.get("products", [])
        return _fmt({"count": len(products), "products": products})
    except Exception as e:
        return _error(e)


class GetProductInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    product_id: int = Field(..., description="The Shopify product ID")


@mcp.tool(
    name="shopify_get_product",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_get_product(params: GetProductInput) -> str:
    """Retrieve a single product by ID, including all variants and images."""
    try:
        data = await _request("GET", f"products/{params.product_id}.json")
        return _fmt(data.get("product", data))
    except Exception as e:
        return _error(e)


class CreateProductInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    title:        str                        = Field(..., min_length=1, description="Product title")
    body_html:    Optional[str]              = Field(default=None, description="HTML description")
    vendor:       Optional[str]              = Field(default=None)
    product_type: Optional[str]              = Field(default=None)
    tags:         Optional[str]              = Field(default=None, description="Comma-separated tags")
    status:       Optional[str]              = Field(default="draft", description="active, archived, or draft")
    variants:     Optional[List[Dict[str, Any]]] = Field(default=None, description="Variant objects with price, sku, etc.")
    options:      Optional[List[Dict[str, Any]]] = Field(default=None, description="Product options (Size, Color, etc.)")
    images:       Optional[List[Dict[str, Any]]] = Field(default=None, description="Image objects with src URL")


@mcp.tool(
    name="shopify_create_product",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def shopify_create_product(params: CreateProductInput) -> str:
    """Create a new product in the Shopify store."""
    try:
        product: Dict[str, Any] = {"title": params.title}
        for field in ["body_html", "vendor", "product_type", "tags", "status", "variants", "options", "images"]:
            val = getattr(params, field)
            if val is not None:
                product[field] = val
        data = await _request("POST", "products.json", body={"product": product})
        return _fmt(data.get("product", data))
    except Exception as e:
        return _error(e)


class UpdateProductInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    product_id:   int            = Field(..., description="Product ID to update")
    title:        Optional[str]  = Field(default=None)
    body_html:    Optional[str]  = Field(default=None)
    vendor:       Optional[str]  = Field(default=None)
    product_type: Optional[str]  = Field(default=None)
    tags:         Optional[str]  = Field(default=None)
    status:       Optional[str]  = Field(default=None, description="active, archived, or draft")
    variants:     Optional[List[Dict[str, Any]]] = Field(default=None)
    handle:       Optional[str]  = Field(default=None, description="URL handle / slug for the product")
    metafields_global_title_tag:       Optional[str] = Field(default=None, description="SEO title tag")
    metafields_global_description_tag: Optional[str] = Field(default=None, description="SEO meta description")

@mcp.tool(
    name="shopify_update_product",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_update_product(params: UpdateProductInput) -> str:
    """Update an existing product. Only provided fields are changed."""
    try:
        product: Dict[str, Any] = {}
        for field in ["title", "body_html", "vendor", "product_type", "tags", "status", "variants", "handle", "metafields_global_title_tag", "metafields_global_description_tag"]:
            val = getattr(params, field)
            if val is not None:
                product[field] = val
        data = await _request("PUT", f"products/{params.product_id}.json", body={"product": product})
        return _fmt(data.get("product", data))
    except Exception as e:
        return _error(e)


class DeleteProductInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    product_id: int = Field(..., description="Product ID to delete")


@mcp.tool(
    name="shopify_delete_product",
    annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_delete_product(params: DeleteProductInput) -> str:
    """Permanently delete a product. This cannot be undone."""
    try:
        await _request("DELETE", f"products/{params.product_id}.json")
        return f"Product {params.product_id} deleted."
    except Exception as e:
        return _error(e)


class ProductCountInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status:       Optional[str] = Field(default=None, description="active, archived, or draft")
    vendor:       Optional[str] = Field(default=None)
    product_type: Optional[str] = Field(default=None)


@mcp.tool(
    name="shopify_count_products",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_count_products(params: ProductCountInput) -> str:
    """Get the total count of products, optionally filtered."""
    try:
        p: Dict[str, Any] = {}
        for field in ["status", "vendor", "product_type"]:
            val = getattr(params, field)
            if val is not None:
                p[field] = val
        data = await _request("GET", "products/count.json", params=p)
        return _fmt(data)
    except Exception as e:
        return _error(e)


# ═══════════════════════════════════════════════════════════════════════════
# ORDERS
# ═══════════════════════════════════════════════════════════════════════════

class ListOrdersInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    limit:               Optional[int] = Field(default=50, ge=1, le=250)
    status:              Optional[str] = Field(default="any", description="open, closed, cancelled, any")
    financial_status:    Optional[str] = Field(default=None, description="authorized, pending, paid, refunded, voided, any")
    fulfillment_status:  Optional[str] = Field(default=None, description="shipped, partial, unshipped, unfulfilled, any")
    since_id:            Optional[int] = Field(default=None)
    created_at_min:      Optional[str] = Field(default=None, description="ISO 8601 date, e.g. 2024-01-01T00:00:00Z")
    created_at_max:      Optional[str] = Field(default=None)
    fields:              Optional[str] = Field(default=None)


@mcp.tool(
    name="shopify_list_orders",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_list_orders(params: ListOrdersInput) -> str:
    """List orders with optional filters for status, financial/fulfillment status, and date range."""
    try:
        p: Dict[str, Any] = {"limit": params.limit, "status": params.status}
        for field in ["financial_status", "fulfillment_status", "since_id", "created_at_min", "created_at_max", "fields"]:
            val = getattr(params, field)
            if val is not None:
                p[field] = val
        data   = await _request("GET", "orders.json", params=p)
        orders = data.get("orders", [])
        return _fmt({"count": len(orders), "orders": orders})
    except Exception as e:
        return _error(e)


class GetOrderInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    order_id: int = Field(..., description="The Shopify order ID")


@mcp.tool(
    name="shopify_get_order",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_get_order(params: GetOrderInput) -> str:
    """Retrieve a single order by ID with full details."""
    try:
        data = await _request("GET", f"orders/{params.order_id}.json")
        return _fmt(data.get("order", data))
    except Exception as e:
        return _error(e)


class OrderCountInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status:             Optional[str] = Field(default="any")
    financial_status:   Optional[str] = Field(default=None)
    fulfillment_status: Optional[str] = Field(default=None)


@mcp.tool(
    name="shopify_count_orders",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_count_orders(params: OrderCountInput) -> str:
    """Get total order count, optionally filtered."""
    try:
        p: Dict[str, Any] = {"status": params.status}
        for field in ["financial_status", "fulfillment_status"]:
            val = getattr(params, field)
            if val is not None:
                p[field] = val
        data = await _request("GET", "orders/count.json", params=p)
        return _fmt(data)
    except Exception as e:
        return _error(e)


class CloseOrderInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    order_id: int = Field(..., description="Order ID to close")


@mcp.tool(
    name="shopify_close_order",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_close_order(params: CloseOrderInput) -> str:
    """Close an order (marks it as completed)."""
    try:
        data = await _request("POST", f"orders/{params.order_id}/close.json")
        return _fmt(data.get("order", data))
    except Exception as e:
        return _error(e)


class CancelOrderInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    order_id: int            = Field(..., description="Order ID to cancel")
    reason:   Optional[str]  = Field(default=None, description="customer, fraud, inventory, declined, other")
    email:    Optional[bool] = Field(default=True,  description="Send cancellation email to customer")
    restock:  Optional[bool] = Field(default=False, description="Restock line items")


@mcp.tool(
    name="shopify_cancel_order",
    annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False, "openWorldHint": True},
)
async def shopify_cancel_order(params: CancelOrderInput) -> str:
    """Cancel an order. Optionally restock items and notify the customer."""
    try:
        body: Dict[str, Any] = {}
        for field in ["reason", "email", "restock"]:
            val = getattr(params, field)
            if val is not None:
                body[field] = val
        data = await _request("POST", f"orders/{params.order_id}/cancel.json", body=body)
        return _fmt(data.get("order", data))
    except Exception as e:
        return _error(e)


# ═══════════════════════════════════════════════════════════════════════════
# CUSTOMERS
# ═══════════════════════════════════════════════════════════════════════════

class ListCustomersInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    limit:          Optional[int] = Field(default=50, ge=1, le=250)
    since_id:       Optional[int] = Field(default=None)
    created_at_min: Optional[str] = Field(default=None, description="ISO 8601 date")
    created_at_max: Optional[str] = Field(default=None)
    fields:         Optional[str] = Field(default=None)


@mcp.tool(
    name="shopify_list_customers",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_list_customers(params: ListCustomersInput) -> str:
    """List customers from the store."""
    try:
        p: Dict[str, Any] = {"limit": params.limit}
        for f in ["since_id", "created_at_min", "created_at_max", "fields"]:
            val = getattr(params, f)
            if val is not None:
                p[f] = val
        data      = await _request("GET", "customers.json", params=p)
        customers = data.get("customers", [])
        return _fmt({"count": len(customers), "customers": customers})
    except Exception as e:
        return _error(e)


class SearchCustomersInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    query: str           = Field(..., min_length=1, description="Search query (name, email, etc.)")
    limit: Optional[int] = Field(default=50, ge=1, le=250)


@mcp.tool(
    name="shopify_search_customers",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_search_customers(params: SearchCustomersInput) -> str:
    """Search customers by name, email, or other fields."""
    try:
        p         = {"query": params.query, "limit": params.limit}
        data      = await _request("GET", "customers/search.json", params=p)
        customers = data.get("customers", [])
        return _fmt({"count": len(customers), "customers": customers})
    except Exception as e:
        return _error(e)


class GetCustomerInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    customer_id: int = Field(..., description="Shopify customer ID")


@mcp.tool(
    name="shopify_get_customer",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_get_customer(params: GetCustomerInput) -> str:
    """Retrieve a single customer by ID."""
    try:
        data = await _request("GET", f"customers/{params.customer_id}.json")
        return _fmt(data.get("customer", data))
    except Exception as e:
        return _error(e)


class CreateCustomerInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    first_name:         Optional[str]  = Field(default=None)
    last_name:          Optional[str]  = Field(default=None)
    email:              Optional[str]  = Field(default=None)
    phone:              Optional[str]  = Field(default=None)
    tags:               Optional[str]  = Field(default=None)
    note:               Optional[str]  = Field(default=None)
    addresses:          Optional[List[Dict[str, Any]]] = Field(default=None)
    send_email_invite:  Optional[bool] = Field(default=False)


@mcp.tool(
    name="shopify_create_customer",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def shopify_create_customer(params: CreateCustomerInput) -> str:
    """Create a new customer."""
    try:
        customer: Dict[str, Any] = {}
        for field in ["first_name", "last_name", "email", "phone", "tags", "note", "addresses", "send_email_invite"]:
            val = getattr(params, field)
            if val is not None:
                customer[field] = val
        data = await _request("POST", "customers.json", body={"customer": customer})
        return _fmt(data.get("customer", data))
    except Exception as e:
        return _error(e)


class UpdateCustomerInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    customer_id: int           = Field(..., description="Customer ID to update")
    first_name:  Optional[str] = Field(default=None)
    last_name:   Optional[str] = Field(default=None)
    email:       Optional[str] = Field(default=None)
    phone:       Optional[str] = Field(default=None)
    tags:        Optional[str] = Field(default=None)
    note:        Optional[str] = Field(default=None)


@mcp.tool(
    name="shopify_update_customer",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_update_customer(params: UpdateCustomerInput) -> str:
    """Update an existing customer. Only provided fields are changed."""
    try:
        customer: Dict[str, Any] = {}
        for field in ["first_name", "last_name", "email", "phone", "tags", "note"]:
            val = getattr(params, field)
            if val is not None:
                customer[field] = val
        data = await _request("PUT", f"customers/{params.customer_id}.json", body={"customer": customer})
        return _fmt(data.get("customer", data))
    except Exception as e:
        return _error(e)


class CustomerOrdersInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    customer_id: int           = Field(..., description="Customer ID")
    limit:       Optional[int] = Field(default=50, ge=1, le=250)
    status:      Optional[str] = Field(default="any")


@mcp.tool(
    name="shopify_get_customer_orders",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_get_customer_orders(params: CustomerOrdersInput) -> str:
    """Get all orders for a specific customer."""
    try:
        p      = {"limit": params.limit, "status": params.status}
        data   = await _request("GET", f"customers/{params.customer_id}/orders.json", params=p)
        orders = data.get("orders", [])
        return _fmt({"count": len(orders), "orders": orders})
    except Exception as e:
        return _error(e)


# ═══════════════════════════════════════════════════════════════════════════
# COLLECTIONS (Custom + Smart)
# ═══════════════════════════════════════════════════════════════════════════

class ListCollectionsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit:           Optional[int] = Field(default=50, ge=1, le=250)
    since_id:        Optional[int] = Field(default=None)
    collection_type: Optional[str] = Field(default="custom", description="'custom' or 'smart'")


@mcp.tool(
    name="shopify_list_collections",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_list_collections(params: ListCollectionsInput) -> str:
    """List custom or smart collections."""
    try:
        endpoint = "custom_collections.json" if params.collection_type == "custom" else "smart_collections.json"
        p: Dict[str, Any] = {"limit": params.limit}
        if params.since_id:
            p["since_id"] = params.since_id
        data = await _request("GET", endpoint, params=p)
        key  = "custom_collections" if params.collection_type == "custom" else "smart_collections"
        collections = data.get(key, [])
        return _fmt({"count": len(collections), "collections": collections})
    except Exception as e:
        return _error(e)


class GetCollectionProductsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    collection_id: int           = Field(..., description="Collection ID")
    limit:         Optional[int] = Field(default=50, ge=1, le=250)


@mcp.tool(
    name="shopify_get_collection_products",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_get_collection_products(params: GetCollectionProductsInput) -> str:
    """Get all products in a specific collection."""
    try:
        p        = {"limit": params.limit, "collection_id": params.collection_id}
        data     = await _request("GET", "products.json", params=p)
        products = data.get("products", [])
        return _fmt({"count": len(products), "products": products})
    except Exception as e:
        return _error(e)


# ═══════════════════════════════════════════════════════════════════════════
# INVENTORY
# ═══════════════════════════════════════════════════════════════════════════

class ListInventoryLocationsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


@mcp.tool(
    name="shopify_list_locations",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_list_locations(params: ListInventoryLocationsInput) -> str:
    """List all inventory locations for the store."""
    try:
        data      = await _request("GET", "locations.json")
        locations = data.get("locations", [])
        return _fmt({"count": len(locations), "locations": locations})
    except Exception as e:
        return _error(e)


class GetInventoryLevelsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    location_id:         Optional[int] = Field(default=None, description="Filter by location ID")
    inventory_item_ids:  Optional[str] = Field(default=None, description="Comma-separated inventory item IDs")


@mcp.tool(
    name="shopify_get_inventory_levels",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_get_inventory_levels(params: GetInventoryLevelsInput) -> str:
    """Get inventory levels for specific locations or inventory items."""
    try:
        p: Dict[str, Any] = {}
        if params.location_id:
            p["location_ids"] = params.location_id
        if params.inventory_item_ids:
            p["inventory_item_ids"] = params.inventory_item_ids
        data   = await _request("GET", "inventory_levels.json", params=p)
        levels = data.get("inventory_levels", [])
        return _fmt({"count": len(levels), "inventory_levels": levels})
    except Exception as e:
        return _error(e)


class SetInventoryLevelInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    inventory_item_id: int = Field(..., description="Inventory item ID")
    location_id:       int = Field(..., description="Location ID")
    available:         int = Field(..., description="Available quantity to set")


@mcp.tool(
    name="shopify_set_inventory_level",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_set_inventory_level(params: SetInventoryLevelInput) -> str:
    """Set the available inventory for an item at a location."""
    try:
        body = {
            "inventory_item_id": params.inventory_item_id,
            "location_id":       params.location_id,
            "available":         params.available,
        }
        data = await _request("POST", "inventory_levels/set.json", body=body)
        return _fmt(data.get("inventory_level", data))
    except Exception as e:
        return _error(e)


# ═══════════════════════════════════════════════════════════════════════════
# FULFILLMENTS
# ═══════════════════════════════════════════════════════════════════════════

class ListFulfillmentsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    order_id: int           = Field(..., description="Order ID")
    limit:    Optional[int] = Field(default=50, ge=1, le=250)


@mcp.tool(
    name="shopify_list_fulfillments",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_list_fulfillments(params: ListFulfillmentsInput) -> str:
    """List fulfillments for a specific order."""
    try:
        p            = {"limit": params.limit}
        data         = await _request("GET", f"orders/{params.order_id}/fulfillments.json", params=p)
        fulfillments = data.get("fulfillments", [])
        return _fmt({"count": len(fulfillments), "fulfillments": fulfillments})
    except Exception as e:
        return _error(e)


class CreateFulfillmentInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    order_id:         int                        = Field(..., description="Order ID to fulfill")
    location_id:      int                        = Field(..., description="Location ID fulfilling from")
    tracking_number:  Optional[str]              = Field(default=None)
    tracking_company: Optional[str]              = Field(default=None, description="e.g. UPS, FedEx, USPS")
    tracking_url:     Optional[str]              = Field(default=None)
    line_items:       Optional[List[Dict[str, Any]]] = Field(default=None, description="Specific line items (omit for all)")
    notify_customer:  Optional[bool]             = Field(default=True, description="Send shipping notification email")


@mcp.tool(
    name="shopify_create_fulfillment",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def shopify_create_fulfillment(params: CreateFulfillmentInput) -> str:
    """Create a fulfillment for an order (ship items)."""
    try:
        fulfillment: Dict[str, Any] = {"location_id": params.location_id}
        for field in ["tracking_number", "tracking_company", "tracking_url", "line_items", "notify_customer"]:
            val = getattr(params, field)
            if val is not None:
                fulfillment[field] = val
        data = await _request(
            "POST",
            f"orders/{params.order_id}/fulfillments.json",
            body={"fulfillment": fulfillment},
        )
        return _fmt(data.get("fulfillment", data))
    except Exception as e:
        return _error(e)


# ═══════════════════════════════════════════════════════════════════════════
# SHOP INFO
# ═══════════════════════════════════════════════════════════════════════════

class EmptyInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


@mcp.tool(
    name="shopify_get_shop",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_get_shop(params: EmptyInput) -> str:
    """Get store information: name, domain, plan, currency, timezone, etc."""
    try:
        data = await _request("GET", "shop.json")
        return _fmt(data.get("shop", data))
    except Exception as e:
        return _error(e)


# ═══════════════════════════════════════════════════════════════════════════
# WEBHOOKS
# ═══════════════════════════════════════════════════════════════════════════

class ListWebhooksInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: Optional[int] = Field(default=50, ge=1, le=250)
    topic: Optional[str] = Field(default=None, description="Filter by topic, e.g. orders/create")


@mcp.tool(
    name="shopify_list_webhooks",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_list_webhooks(params: ListWebhooksInput) -> str:
    """List configured webhooks."""
    try:
        p: Dict[str, Any] = {"limit": params.limit}
        if params.topic:
            p["topic"] = params.topic
        data     = await _request("GET", "webhooks.json", params=p)
        webhooks = data.get("webhooks", [])
        return _fmt({"count": len(webhooks), "webhooks": webhooks})
    except Exception as e:
        return _error(e)


class CreateWebhookInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    topic:   str           = Field(..., description="Webhook topic, e.g. orders/create, products/update")
    address: str           = Field(..., description="URL to receive the webhook POST")
    format:  Optional[str] = Field(default="json", description="json or xml")


@mcp.tool(
    name="shopify_create_webhook",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def shopify_create_webhook(params: CreateWebhookInput) -> str:
    """Create a new webhook subscription."""
    try:
        webhook = {"topic": params.topic, "address": params.address, "format": params.format}
        data    = await _request("POST", "webhooks.json", body={"webhook": webhook})
        return _fmt(data.get("webhook", data))
    except Exception as e:
        return _error(e)

class CreateDiscountCodeInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    code: str = Field(..., description="The discount code, e.g. WELCOME10")
    discount_type: str = Field(..., description="percentage or fixed_amount")
    value: float = Field(..., description="Discount value (e.g. 10 for 10%)")
    usage_limit: Optional[int] = Field(default=1, description="Max total uses (None = unlimited)")
    once_per_customer: bool = Field(default=True, description="Limit to one use per customer")
    starts_at: Optional[str] = Field(default=None, description="ISO 8601 start date, e.g. 2025-01-01T00:00:00Z")
    ends_at: Optional[str] = Field(default=None, description="ISO 8601 expiry date")

@mcp.tool(
    name="shopify_create_discount_code",
    annotations={"readOnlyHint": False, "destructiveHint": False,
                 "idempotentHint": False, "openWorldHint": True},
)
async def shopify_create_discount_code(params: CreateDiscountCodeInput) -> str:
    """Create a discount code with a price rule. Handles both percentage and fixed amount discounts."""
    try:
        price_rule_body = {
            "price_rule": {
                "title": params.code,
                "target_type": "line_item",
                "target_selection": "all",
                "allocation_method": "across",
                "value_type": params.discount_type,
                "value": f"-{abs(params.value)}",
                "customer_selection": "all",
                "usage_limit": params.usage_limit,
                "once_per_customer": params.once_per_customer,
                "starts_at": params.starts_at or "2020-01-01T00:00:00Z",
                **({"ends_at": params.ends_at} if params.ends_at else {}),
            }
        }
        rule = await _request("POST", "price_rules.json", body=price_rule_body)
        rule_id = rule["price_rule"]["id"]

        code_body = {"discount_code": {"code": params.code}}
        code = await _request("POST", f"price_rules/{rule_id}/discount_codes.json", body=code_body)
        return _fmt({"price_rule": rule["price_rule"], "discount_code": code["discount_code"]})
    except Exception as e:
        return _error(e)


# ═══════════════════════════════════════════════════════════════════════════
# THEMES
# ═══════════════════════════════════════════════════════════════════════════

class ListThemesInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    fields: Optional[str] = Field(default=None, description="Comma-separated fields to include, e.g. id,name,role")


@mcp.tool(
    name="shopify_list_themes",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_list_themes(params: ListThemesInput) -> str:
    """List all themes in the store. The theme with role 'main' is the published theme."""
    try:
        p: Dict[str, Any] = {}
        if params.fields:
            p["fields"] = params.fields
        data = await _request("GET", "themes.json", params=p)
        themes = data.get("themes", [])
        return _fmt({"count": len(themes), "themes": themes})
    except Exception as e:
        return _error(e)


class GetThemeInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    theme_id: int = Field(..., description="The theme ID")


@mcp.tool(
    name="shopify_get_theme",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_get_theme(params: GetThemeInput) -> str:
    """Get a single theme by ID."""
    try:
        data = await _request("GET", f"themes/{params.theme_id}.json")
        return _fmt(data.get("theme", data))
    except Exception as e:
        return _error(e)


class ListThemeAssetsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    theme_id: int = Field(..., description="The theme ID")


@mcp.tool(
    name="shopify_list_theme_assets",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_list_theme_assets(params: ListThemeAssetsInput) -> str:
    """List all asset keys (file paths) in a theme. Returns keys like templates/index.json, sections/header.liquid, etc."""
    try:
        data = await _request("GET", f"themes/{params.theme_id}/assets.json")
        assets = data.get("assets", [])
        return _fmt({"count": len(assets), "assets": assets})
    except Exception as e:
        return _error(e)


class GetThemeAssetInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    theme_id: int = Field(..., description="The theme ID")
    asset_key: str = Field(..., description="Asset key path, e.g. templates/index.json, sections/header.liquid, config/settings_data.json")


@mcp.tool(
    name="shopify_get_theme_asset",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_get_theme_asset(params: GetThemeAssetInput) -> str:
    """Read a single theme file by its asset key. Returns the file content."""
    try:
        data = await _request("GET", f"themes/{params.theme_id}/assets.json", params={"asset[key]": params.asset_key})
        return _fmt(data.get("asset", data))
    except Exception as e:
        return _error(e)


class UpdateThemeAssetInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    theme_id: int = Field(..., description="The theme ID")
    asset_key: str = Field(..., description="Asset key path, e.g. templates/product.liquid, sections/header.liquid")
    value: Optional[str] = Field(default=None, description="The asset content (Liquid, JSON, CSS, JS, etc.)")
    src: Optional[str] = Field(default=None, description="URL source for the asset (for images/binary files)")


@mcp.tool(
    name="shopify_update_theme_asset",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_update_theme_asset(params: UpdateThemeAssetInput) -> str:
    """Create or update a theme asset (file). Use 'value' for text content (Liquid, JSON, CSS, JS) or 'src' for a URL to an image/binary file."""
    try:
        asset: Dict[str, Any] = {"key": params.asset_key}
        if params.value is not None:
            asset["value"] = params.value
        elif params.src is not None:
            asset["src"] = params.src
        else:
            return "Error: provide either 'value' (file content) or 'src' (URL)."
        data = await _request("PUT", f"themes/{params.theme_id}/assets.json", body={"asset": asset})
        return _fmt(data.get("asset", data))
    except Exception as e:
        return _error(e)


class DeleteThemeAssetInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    theme_id: int = Field(..., description="The theme ID")
    asset_key: str = Field(..., description="Asset key path to delete")


@mcp.tool(
    name="shopify_delete_theme_asset",
    annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False, "openWorldHint": True},
)
async def shopify_delete_theme_asset(params: DeleteThemeAssetInput) -> str:
    """Delete a theme asset (file). WARNING: this is destructive and cannot be undone."""
    try:
        await _request("DELETE", f"themes/{params.theme_id}/assets.json", params={"asset[key]": params.asset_key})
        return _fmt({"deleted": params.asset_key})
    except Exception as e:
        return _error(e)


# ---------------------------------------------------------------------------
# NAVIGATION / MENUS (GraphQL Admin API)
# Requires scopes: read_online_store_navigation, write_online_store_navigation
# ---------------------------------------------------------------------------

class ListMenusInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: int = Field(default=25, ge=1, le=100, description="Max menus to return")


@mcp.tool(
    name="shopify_list_menus",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_list_menus(params: ListMenusInput) -> str:
    """List all navigation menus in the online store."""
    try:
        query = """
        query listMenus($first: Int!) {
          menus(first: $first) {
            edges {
              node {
                id
                title
                handle
              }
            }
          }
        }
        """
        data = await _graphql(query, variables={"first": params.limit})
        menus = [edge["node"] for edge in data.get("menus", {}).get("edges", [])]
        return _fmt({"count": len(menus), "menus": menus})
    except Exception as e:
        return _error(e)


class GetMenuInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    handle: str = Field(..., description="Menu handle, e.g. 'main-menu' or 'footer'")


@mcp.tool(
    name="shopify_get_menu",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_get_menu(params: GetMenuInput) -> str:
    """Get a navigation menu by handle, including all items and nested sub-items."""
    try:
        query = """
        query getMenu($handle: String!) {
          menuByHandle(handle: $handle) {
            id
            title
            handle
            itemsCount
            items {
              id
              title
              type
              url
              resourceId
              tags
              items {
                id
                title
                type
                url
                resourceId
                tags
              }
            }
          }
        }
        """
        data = await _graphql(query, variables={"handle": params.handle})
        menu = data.get("menuByHandle")
        if not menu:
            return _fmt({"error": f"Menu with handle '{params.handle}' not found"})
        return _fmt(menu)
    except Exception as e:
        return _error(e)


class MenuItemInput(BaseModel):
    """A single menu item (can include nested sub-items for dropdowns)."""
    model_config = ConfigDict(extra="forbid")
    title: str = Field(..., description="Display text for the menu item")
    type: str = Field(default="HTTP", description="Item type: HTTP, COLLECTION, PRODUCT, PAGE, BLOG, SHOP_POLICY, CATALOG")
    url: Optional[str] = Field(default=None, description="URL for HTTP type items (e.g. '/collections/dresses')")
    resource_id: Optional[str] = Field(default=None, description="Shopify GID for resource types (e.g. 'gid://shopify/Collection/12345')")
    items: Optional[List["MenuItemInput"]] = Field(default=None, description="Nested sub-items for dropdown menus")


MenuItemInput.model_rebuild()


class CreateMenuInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: str = Field(..., description="Menu title (e.g. 'Main menu')")
    handle: str = Field(..., description="Menu handle (e.g. 'main-menu')")
    items: List[MenuItemInput] = Field(..., description="Menu items to create")


def _build_menu_items(items: List[MenuItemInput]) -> List[dict]:
    """Convert Pydantic menu items to GraphQL MenuItemCreateInput format."""
    result = []
    for item in items:
        entry: Dict[str, Any] = {"title": item.title, "type": item.type}
        if item.url:
            entry["url"] = item.url
        if item.resource_id:
            entry["resourceId"] = item.resource_id
        if item.items:
            entry["items"] = _build_menu_items(item.items)
        result.append(entry)
    return result


@mcp.tool(
    name="shopify_create_menu",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def shopify_create_menu(params: CreateMenuInput) -> str:
    """Create a new navigation menu with items. Supports nested items for dropdown menus.

    Item types:
      - HTTP: custom URL (provide 'url' field, e.g. '/collections/dresses')
      - COLLECTION: link to a collection (provide 'resource_id' as GID)
      - PRODUCT: link to a product (provide 'resource_id' as GID)
      - PAGE: link to a page (provide 'resource_id' as GID)
      - BLOG: link to a blog (provide 'resource_id' as GID)
      - CATALOG: link to a catalog

    For dropdown menus, nest items inside a parent item's 'items' array.
    """
    try:
        mutation = """
        mutation menuCreate($title: String!, $handle: String!, $items: [MenuItemCreateInput!]!) {
          menuCreate(title: $title, handle: $handle, items: $items) {
            menu {
              id
              title
              handle
              itemsCount
              items {
                id
                title
                type
                url
                items {
                  id
                  title
                  type
                  url
                }
              }
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        variables = {
            "title": params.title,
            "handle": params.handle,
            "items": _build_menu_items(params.items),
        }
        data = await _graphql(mutation, variables=variables)
        result = data.get("menuCreate", {})
        if result.get("userErrors"):
            return _fmt({"errors": result["userErrors"]})
        return _fmt(result.get("menu", result))
    except Exception as e:
        return _error(e)


class UpdateMenuInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(..., description="Menu GID (e.g. 'gid://shopify/Menu/12345')")
    title: Optional[str] = Field(default=None, description="New menu title")
    items: Optional[List[MenuItemInput]] = Field(default=None, description="Complete list of menu items (replaces existing)")


@mcp.tool(
    name="shopify_update_menu",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_update_menu(params: UpdateMenuInput) -> str:
    """Update an existing navigation menu. Pass items to replace all menu items.
    Note: items array replaces ALL existing items — include all items you want to keep.
    """
    try:
        mutation = """
        mutation menuUpdate($id: ID!, $title: String, $items: [MenuItemCreateInput!]) {
          menuUpdate(id: $id, title: $title, items: $items) {
            menu {
              id
              title
              handle
              itemsCount
              items {
                id
                title
                type
                url
                items {
                  id
                  title
                  type
                  url
                }
              }
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        variables: Dict[str, Any] = {"id": params.id}
        if params.title is not None:
            variables["title"] = params.title
        if params.items is not None:
            variables["items"] = _build_menu_items(params.items)

        data = await _graphql(mutation, variables=variables)
        result = data.get("menuUpdate", {})
        if result.get("userErrors"):
            return _fmt({"errors": result["userErrors"]})
        return _fmt(result.get("menu", result))
    except Exception as e:
        return _error(e)


class DeleteMenuInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(..., description="Menu GID to delete (e.g. 'gid://shopify/Menu/12345')")


@mcp.tool(
    name="shopify_delete_menu",
    annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False, "openWorldHint": True},
)
async def shopify_delete_menu(params: DeleteMenuInput) -> str:
    """Delete a navigation menu. WARNING: this is destructive and cannot be undone."""
    try:
        mutation = """
        mutation menuDelete($id: ID!) {
          menuDelete(id: $id) {
            deletedMenuId
            userErrors {
              field
              message
            }
          }
        }
        """
        data = await _graphql(mutation, variables={"id": params.id})
        result = data.get("menuDelete", {})
        if result.get("userErrors"):
            return _fmt({"errors": result["userErrors"]})
        return _fmt({"deleted": result.get("deletedMenuId")})
    except Exception as e:
        return _error(e)


# ---------------------------------------------------------------------------
# Pages  (REST — requires read_online_store_pages / write_online_store_pages)
# ---------------------------------------------------------------------------

class ListPagesInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: int = Field(50, ge=1, le=250, description="Max pages to return (1-250)")
    since_id: Optional[int] = Field(None, description="Restrict results to after this ID")
    title: Optional[str] = Field(None, description="Filter by page title")
    published_status: Optional[str] = Field(None, description="Filter: 'published', 'unpublished', or 'any'")


@mcp.tool(
    name="shopify_list_pages",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_list_pages(params: ListPagesInput) -> str:
    """List store pages (About, Contact, FAQ, etc.)."""
    try:
        qp: Dict[str, Any] = {"limit": params.limit}
        if params.since_id:
            qp["since_id"] = params.since_id
        if params.title:
            qp["title"] = params.title
        if params.published_status:
            qp["published_status"] = params.published_status
        data = await _request("GET", "pages.json", params=qp)
        return _fmt(data)
    except Exception as e:
        return _error(e)


class GetPageInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    page_id: int = Field(..., description="Page ID")


@mcp.tool(
    name="shopify_get_page",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_get_page(params: GetPageInput) -> str:
    """Get a single page by ID with full HTML body."""
    try:
        data = await _request("GET", f"pages/{params.page_id}.json")
        return _fmt(data)
    except Exception as e:
        return _error(e)


class CreatePageInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: str = Field(..., description="Page title")
    body_html: Optional[str] = Field(None, description="HTML content of the page")
    published: bool = Field(True, description="Whether the page is visible (default true)")
    template_suffix: Optional[str] = Field(None, description="Liquid template suffix (e.g. 'contact')")
    handle: Optional[str] = Field(None, description="URL handle (auto-generated from title if omitted)")


@mcp.tool(
    name="shopify_create_page",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def shopify_create_page(params: CreatePageInput) -> str:
    """Create a new page (About, Contact, FAQ, etc.)."""
    try:
        page: Dict[str, Any] = {"title": params.title, "published": params.published}
        if params.body_html is not None:
            page["body_html"] = params.body_html
        if params.template_suffix:
            page["template_suffix"] = params.template_suffix
        if params.handle:
            page["handle"] = params.handle
        data = await _request("POST", "pages.json", json={"page": page})
        return _fmt(data)
    except Exception as e:
        return _error(e)


class UpdatePageInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    page_id: int = Field(..., description="Page ID to update")
    title: Optional[str] = Field(None, description="New title")
    body_html: Optional[str] = Field(None, description="New HTML content")
    published: Optional[bool] = Field(None, description="Published status")
    template_suffix: Optional[str] = Field(None, description="Liquid template suffix")
    handle: Optional[str] = Field(None, description="URL handle")


@mcp.tool(
    name="shopify_update_page",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_update_page(params: UpdatePageInput) -> str:
    """Update an existing page's title, body, or published status."""
    try:
        page: Dict[str, Any] = {}
        if params.title is not None:
            page["title"] = params.title
        if params.body_html is not None:
            page["body_html"] = params.body_html
        if params.published is not None:
            page["published"] = params.published
        if params.template_suffix is not None:
            page["template_suffix"] = params.template_suffix
        if params.handle is not None:
            page["handle"] = params.handle
        if not page:
            return _fmt({"error": "No fields to update"})
        data = await _request("PUT", f"pages/{params.page_id}.json", json={"page": page})
        return _fmt(data)
    except Exception as e:
        return _error(e)


class DeletePageInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    page_id: int = Field(..., description="Page ID to delete")


@mcp.tool(
    name="shopify_delete_page",
    annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False, "openWorldHint": True},
)
async def shopify_delete_page(params: DeletePageInput) -> str:
    """Delete a page. WARNING: this is destructive and cannot be undone."""
    try:
        await _request("DELETE", f"pages/{params.page_id}.json")
        return _fmt({"deleted": True, "page_id": params.page_id})
    except Exception as e:
        return _error(e)


# ---------------------------------------------------------------------------
# Blogs & Articles  (REST — requires read_content / write_content)
# ---------------------------------------------------------------------------

class ListBlogsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: int = Field(50, ge=1, le=250, description="Max blogs to return")


@mcp.tool(
    name="shopify_list_blogs",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_list_blogs(params: ListBlogsInput) -> str:
    """List all blogs (e.g. 'News', 'Stories')."""
    try:
        data = await _request("GET", "blogs.json", params={"limit": params.limit})
        return _fmt(data)
    except Exception as e:
        return _error(e)


class ListArticlesInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    blog_id: int = Field(..., description="Blog ID to list articles from")
    limit: int = Field(50, ge=1, le=250, description="Max articles to return")
    published_status: Optional[str] = Field(None, description="Filter: 'published', 'unpublished', or 'any'")


@mcp.tool(
    name="shopify_list_articles",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_list_articles(params: ListArticlesInput) -> str:
    """List articles in a blog."""
    try:
        qp: Dict[str, Any] = {"limit": params.limit}
        if params.published_status:
            qp["published_status"] = params.published_status
        data = await _request("GET", f"blogs/{params.blog_id}/articles.json", params=qp)
        return _fmt(data)
    except Exception as e:
        return _error(e)


class GetArticleInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    blog_id: int = Field(..., description="Blog ID")
    article_id: int = Field(..., description="Article ID")


@mcp.tool(
    name="shopify_get_article",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_get_article(params: GetArticleInput) -> str:
    """Get a single blog article with full body."""
    try:
        data = await _request("GET", f"blogs/{params.blog_id}/articles/{params.article_id}.json")
        return _fmt(data)
    except Exception as e:
        return _error(e)


class CreateArticleInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    blog_id: int = Field(..., description="Blog ID to create the article in")
    title: str = Field(..., description="Article title")
    body_html: Optional[str] = Field(None, description="HTML content")
    author: Optional[str] = Field(None, description="Author name")
    tags: Optional[str] = Field(None, description="Comma-separated tags")
    summary_html: Optional[str] = Field(None, description="Short summary/excerpt HTML")
    published: bool = Field(True, description="Whether the article is visible")
    handle: Optional[str] = Field(None, description="URL handle")


@mcp.tool(
    name="shopify_create_article",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def shopify_create_article(params: CreateArticleInput) -> str:
    """Create a new blog article."""
    try:
        article: Dict[str, Any] = {"title": params.title, "published": params.published}
        if params.body_html is not None:
            article["body_html"] = params.body_html
        if params.author:
            article["author"] = params.author
        if params.tags:
            article["tags"] = params.tags
        if params.summary_html:
            article["summary_html"] = params.summary_html
        if params.handle:
            article["handle"] = params.handle
        data = await _request("POST", f"blogs/{params.blog_id}/articles.json", json={"article": article})
        return _fmt(data)
    except Exception as e:
        return _error(e)


class UpdateArticleInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    blog_id: int = Field(..., description="Blog ID")
    article_id: int = Field(..., description="Article ID to update")
    title: Optional[str] = Field(None, description="New title")
    body_html: Optional[str] = Field(None, description="New HTML content")
    author: Optional[str] = Field(None, description="Author name")
    tags: Optional[str] = Field(None, description="Comma-separated tags")
    summary_html: Optional[str] = Field(None, description="Short summary/excerpt HTML")
    published: Optional[bool] = Field(None, description="Published status")
    handle: Optional[str] = Field(None, description="URL handle")


@mcp.tool(
    name="shopify_update_article",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_update_article(params: UpdateArticleInput) -> str:
    """Update an existing blog article."""
    try:
        article: Dict[str, Any] = {}
        if params.title is not None:
            article["title"] = params.title
        if params.body_html is not None:
            article["body_html"] = params.body_html
        if params.author is not None:
            article["author"] = params.author
        if params.tags is not None:
            article["tags"] = params.tags
        if params.summary_html is not None:
            article["summary_html"] = params.summary_html
        if params.published is not None:
            article["published"] = params.published
        if params.handle is not None:
            article["handle"] = params.handle
        if not article:
            return _fmt({"error": "No fields to update"})
        data = await _request("PUT", f"blogs/{params.blog_id}/articles/{params.article_id}.json", json={"article": article})
        return _fmt(data)
    except Exception as e:
        return _error(e)


class DeleteArticleInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    blog_id: int = Field(..., description="Blog ID")
    article_id: int = Field(..., description="Article ID to delete")


@mcp.tool(
    name="shopify_delete_article",
    annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False, "openWorldHint": True},
)
async def shopify_delete_article(params: DeleteArticleInput) -> str:
    """Delete a blog article. WARNING: destructive and cannot be undone."""
    try:
        await _request("DELETE", f"blogs/{params.blog_id}/articles/{params.article_id}.json")
        return _fmt({"deleted": True, "blog_id": params.blog_id, "article_id": params.article_id})
    except Exception as e:
        return _error(e)


# ---------------------------------------------------------------------------
# Policies  (REST read + GraphQL update — requires read/write_legal_policies)
# ---------------------------------------------------------------------------

class ListPoliciesInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


@mcp.tool(
    name="shopify_list_policies",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_list_policies(params: ListPoliciesInput) -> str:
    """List all store legal policies (privacy, refund, terms, shipping, etc.)."""
    try:
        data = await _request("GET", "policies.json")
        return _fmt(data)
    except Exception as e:
        return _error(e)


class UpdatePolicyInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: str = Field(
        ...,
        description=(
            "Policy type: REFUND_POLICY, PRIVACY_POLICY, TERMS_OF_SERVICE, "
            "SHIPPING_POLICY, LEGAL_NOTICE, SUBSCRIPTION_POLICY, CONTACT_INFORMATION"
        ),
    )
    body: str = Field(..., description="HTML content of the policy")


@mcp.tool(
    name="shopify_update_policy",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_update_policy(params: UpdatePolicyInput) -> str:
    """Update a store legal policy (privacy, refund, terms, etc.) via GraphQL."""
    try:
        mutation = """
        mutation shopPolicyUpdate($shopPolicy: ShopPolicyInput!) {
          shopPolicyUpdate(shopPolicy: $shopPolicy) {
            shopPolicy {
              id
              type
              body
              url
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        variables = {
            "shopPolicy": {
                "type": params.type,
                "body": params.body,
            }
        }
        data = await _graphql(mutation, variables=variables)
        result = data.get("shopPolicyUpdate", {})
        if result.get("userErrors"):
            return _fmt({"errors": result["userErrors"]})
        return _fmt(result.get("shopPolicy", result))
    except Exception as e:
        return _error(e)


# ---------------------------------------------------------------------------
# Markets  (REST — requires read_markets / write_markets)
# ---------------------------------------------------------------------------

class ListMarketsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


@mcp.tool(
    name="shopify_list_markets",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_list_markets(params: ListMarketsInput) -> str:
    """List all markets (regions/countries the store sells to)."""
    try:
        data = await _request("GET", "markets.json")
        return _fmt(data)
    except Exception as e:
        return _error(e)


class GetMarketInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    market_id: int = Field(..., description="Market ID")


@mcp.tool(
    name="shopify_get_market",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_get_market(params: GetMarketInput) -> str:
    """Get a single market by ID (regions, currencies, domains)."""
    try:
        data = await _request("GET", f"markets/{params.market_id}.json")
        return _fmt(data)
    except Exception as e:
        return _error(e)


class UpdateMarketInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    market_id: int = Field(..., description="Market ID to update")
    name: Optional[str] = Field(None, description="Market name")
    enabled: Optional[bool] = Field(None, description="Enable or disable the market")


@mcp.tool(
    name="shopify_update_market",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_update_market(params: UpdateMarketInput) -> str:
    """Update a market's name or enabled status."""
    try:
        market: Dict[str, Any] = {}
        if params.name is not None:
            market["name"] = params.name
        if params.enabled is not None:
            market["enabled"] = params.enabled
        if not market:
            return _fmt({"error": "No fields to update"})
        data = await _request("PUT", f"markets/{params.market_id}.json", json={"market": market})
        return _fmt(data)
    except Exception as e:
        return _error(e)


# ---------------------------------------------------------------------------
# Redirects  (REST — requires read_content / write_content)
# ---------------------------------------------------------------------------

class ListRedirectsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    limit: int = Field(50, ge=1, le=250, description="Max redirects to return")
    path: Optional[str] = Field(None, description="Filter by source path")
    target: Optional[str] = Field(None, description="Filter by target path")


@mcp.tool(
    name="shopify_list_redirects",
    annotations={"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True, "openWorldHint": True},
)
async def shopify_list_redirects(params: ListRedirectsInput) -> str:
    """List URL redirects (301 redirects for SEO)."""
    try:
        qp: Dict[str, Any] = {"limit": params.limit}
        if params.path:
            qp["path"] = params.path
        if params.target:
            qp["target"] = params.target
        data = await _request("GET", "redirects.json", params=qp)
        return _fmt(data)
    except Exception as e:
        return _error(e)


class CreateRedirectInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str = Field(..., description="Source path (e.g. '/old-page')")
    target: str = Field(..., description="Target path or full URL (e.g. '/new-page')")


@mcp.tool(
    name="shopify_create_redirect",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False, "openWorldHint": True},
)
async def shopify_create_redirect(params: CreateRedirectInput) -> str:
    """Create a URL redirect (301) for SEO."""
    try:
        data = await _request("POST", "redirects.json", json={
            "redirect": {"path": params.path, "target": params.target}
        })
        return _fmt(data)
    except Exception as e:
        return _error(e)


class DeleteRedirectInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    redirect_id: int = Field(..., description="Redirect ID to delete")


@mcp.tool(
    name="shopify_delete_redirect",
    annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False, "openWorldHint": True},
)
async def shopify_delete_redirect(params: DeleteRedirectInput) -> str:
    """Delete a URL redirect. WARNING: destructive."""
    try:
        await _request("DELETE", f"redirects/{params.redirect_id}.json")
        return _fmt({"deleted": True, "redirect_id": params.redirect_id})
    except Exception as e:
        return _error(e)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    mcp.run(transport=MCP_TRANSPORT)
