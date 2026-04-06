import io
import zipfile

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

router = APIRouter(prefix="/api/plugin", tags=["plugin"])

PLUGIN_PHP = """<?php
/**
 * Plugin Name: Price Resolver Bridge
 * Description: Heartbeat + REST bridge (MVP)
 * Version: 0.1.0
 */
add_action('rest_api_init', function () {
    register_rest_route('price-resolver/v1', '/ping', [
        'methods' => 'GET',
        'callback' => function () {
            return ['ok' => true, 'site' => get_site_url(), 'ts' => time()];
        },
        'permission_callback' => '__return_true',
    ]);
});
"""


@router.get("/download-zip")
def download_zip() -> StreamingResponse:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("price-resolver-bridge/price-resolver-bridge.php", PLUGIN_PHP)
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="price-resolver-bridge.zip"'},
    )
