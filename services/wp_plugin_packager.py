"""בניית ZIP של תוסף WordPress עם טוקן הקמה מוטמע."""

from __future__ import annotations

import io
import zipfile

PLUGIN_MAIN_PHP = r"""<?php
/**
 * Plugin Name: Price Resolver Connect
 * Description: חיבור אוטומטי ל-Price Resolver — יוצר מפתחות WooCommerce REST ומעביר למערכת.
 * Version: 1.0.0
 * Requires at least: 5.8
 * Requires PHP: 7.4
 */
if (!defined('ABSPATH')) {
    exit;
}
if (!file_exists(__DIR__ . '/pr-config.php')) {
    add_action('admin_notices', function () {
        echo '<div class="error"><p>Price Resolver: חסר pr-config.php — הורידו שוב מהמערכת.</p></div>';
    });
    return;
}
require_once __DIR__ . '/pr-config.php';
if (!defined('PR_API_BASE') || !defined('PR_SETUP_TOKEN')) {
    return;
}

add_action('admin_menu', function () {
    add_submenu_page(
        'woocommerce',
        'Price Resolver',
        'Price Resolver',
        'manage_woocommerce',
        'price-resolver-connect',
        'pr_render_admin_page'
    );
});

function pr_render_admin_page(): void
{
    if (!current_user_can('manage_woocommerce')) {
        wp_die('אין הרשאה');
    }
    $done = isset($_GET['pr_done']) ? sanitize_text_field((string) $_GET['pr_done']) : '';
    ?>
    <div class="wrap">
        <h1>Price Resolver — חיבור לחנות</h1>
        <p>לחצו על הכפתור כדי ליצור מפתחות API ב-WooCommerce ולשלוח אותם למערכת המעקב אוטומטית.</p>
        <?php if ($done === '1') : ?>
            <div class="notice notice-success"><p>החנות חוברה בהצלחה. חזרו למערכת Price Resolver.</p></div>
        <?php elseif ($done === 'err') : ?>
            <div class="notice notice-error"><p>החיבור נכשל. בדקו שה-API זמין ונסו שוב.</p></div>
        <?php endif; ?>
        <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>">
            <input type="hidden" name="action" value="pr_connect_submit" />
            <?php wp_nonce_field('pr_connect'); ?>
            <?php submit_button('חבר עכשיו (יצירת מפתחות + שליחה)', 'primary', 'submit', false); ?>
        </form>
    </div>
    <?php
}

add_action('admin_post_pr_connect_submit', function () {
    if (!current_user_can('manage_woocommerce')) {
        wp_die('אין הרשאה');
    }
    check_admin_referer('pr_connect');
    if (!function_exists('wc_rand_hash') || !function_exists('wc_api_hash') || !function_exists('wc_hash')) {
        wp_safe_redirect(admin_url('admin.php?page=price-resolver-connect&pr_done=err'));
        exit;
    }
    global $wpdb;
    $user_id = get_current_user_id();
    $consumer_key = 'ck_' . wc_rand_hash();
    $consumer_secret = 'cs_' . wc_rand_hash();
    $table = $wpdb->prefix . 'woocommerce_api_keys';
    $ok = $wpdb->insert(
        $table,
        array(
            'user_id' => $user_id,
            'description' => 'Price Resolver',
            'permissions' => 'read_write',
            'consumer_key' => wc_api_hash($consumer_key),
            'consumer_secret' => wc_hash($consumer_secret),
            'truncated_key' => substr($consumer_key, -7),
        ),
        array('%d', '%s', '%s', '%s', '%s', '%s')
    );
    if (!$ok) {
        wp_safe_redirect(admin_url('admin.php?page=price-resolver-connect&pr_done=err'));
        exit;
    }
    $site = untrailingslashit(home_url());
    $payload = array(
        'setup_token' => PR_SETUP_TOKEN,
        'site_url' => $site,
        'consumer_key' => $consumer_key,
        'consumer_secret' => $consumer_secret,
    );
    $url = rtrim(PR_API_BASE, '/') . '/api/integrations/wordpress/connect';
    $res = wp_remote_post(
        $url,
        array(
            'timeout' => 45,
            'headers' => array('Content-Type' => 'application/json; charset=utf-8'),
            'body' => wp_json_encode($payload),
        )
    );
    if (is_wp_error($res)) {
        wp_safe_redirect(admin_url('admin.php?page=price-resolver-connect&pr_done=err'));
        exit;
    }
    $code = wp_remote_retrieve_response_code($res);
    if ($code < 200 || $code >= 300) {
        wp_safe_redirect(admin_url('admin.php?page=price-resolver-connect&pr_done=err'));
        exit;
    }
    wp_safe_redirect(admin_url('admin.php?page=price-resolver-connect&pr_done=1'));
    exit;
});
"""


def build_plugin_zip_bytes(api_base: str, setup_token: str) -> bytes:
    config_php = f"""<?php
define('PR_API_BASE', {repr(api_base)});
define('PR_SETUP_TOKEN', {repr(setup_token)});
"""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("price-resolver-connect/pr-config.php", config_php)
        z.writestr("price-resolver-connect/price-resolver-connect.php", PLUGIN_MAIN_PHP)
    buf.seek(0)
    return buf.read()
