"""בניית ZIP של תוסף WordPress עם טוקן הקמה מוטמע."""

from __future__ import annotations

import io
import zipfile

PLUGIN_MAIN_PHP = r"""<?php
/**
 * Plugin Name: Price Resolver Connect
 * Description: חיבור אוטומטי ל-Price Resolver — יוצר מפתחות WooCommerce REST ומעביר למערכת.
 * Version: 1.0.3
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
    $saved_api = trim((string) get_option('pr_api_base_override', ''));
    $effective_api = $saved_api !== '' ? $saved_api : (defined('PR_API_BASE') ? (string) PR_API_BASE : '');
    $api_host = (string) wp_parse_url($effective_api, PHP_URL_HOST);
    ?>
    <div class="wrap">
        <h1>Price Resolver — חיבור לחנות</h1>
        <p>לחצו על הכפתור כדי ליצור מפתחות API ב-WooCommerce ולשלוח אותם למערכת המעקב אוטומטית.</p>
        <p><strong>API בפועל:</strong> <code dir="ltr"><?php echo esc_html($effective_api); ?></code></p>
        <?php if ($api_host === '127.0.0.1' || $api_host === 'localhost') : ?>
            <div class="notice notice-warning"><p>
                ה־API מוגדר ל־localhost/127.0.0.1 ולכן שרת וורדפרס לא יכול להתחבר אליו. הזינו כתובת API ציבורית למטה.
            </p></div>
        <?php endif; ?>
        <?php
        $last_err = get_transient('pr_last_error');
        if ($last_err) {
            delete_transient('pr_last_error');
        }
        ?>
        <?php if ($done === '1') : ?>
            <div class="notice notice-success"><p>החנות חוברה בהצלחה. חזרו למערכת Price Resolver.</p></div>
        <?php elseif ($done === 'err') : ?>
            <div class="notice notice-error">
                <p><strong>החיבור נכשל.</strong> בדקו שה-API זמין ונסו שוב.</p>
                <?php if ($last_err) : ?>
                    <p style="direction:ltr;max-width:920px;white-space:pre-wrap;"><?php echo esc_html((string) $last_err); ?></p>
                <?php endif; ?>
            </div>
        <?php endif; ?>
        <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>">
            <input type="hidden" name="action" value="pr_connect_save_api_base" />
            <?php wp_nonce_field('pr_connect_save_api'); ?>
            <table class="form-table" role="presentation">
                <tr>
                    <th scope="row"><label for="pr_api_base_override">כתובת API ציבורית</label></th>
                    <td>
                        <input
                            type="url"
                            id="pr_api_base_override"
                            name="pr_api_base_override"
                            value="<?php echo esc_attr($saved_api); ?>"
                            class="regular-text"
                            placeholder="https://api.example.com"
                            dir="ltr"
                        />
                        <p class="description">אם מוגדר כאן ערך — הוא עוקף את הכתובת שמגיעה בקובץ ZIP.</p>
                    </td>
                </tr>
            </table>
            <?php submit_button('שמור כתובת API', 'secondary', 'submit', false); ?>
        </form>
        <form method="post" action="<?php echo esc_url(admin_url('admin-post.php')); ?>" style="margin-top:12px;">
            <input type="hidden" name="action" value="pr_connect_submit" />
            <?php wp_nonce_field('pr_connect'); ?>
            <?php submit_button('חבר עכשיו (יצירת מפתחות + שליחה)', 'primary', 'submit', false); ?>
        </form>
    </div>
    <?php
}

add_action('admin_post_pr_connect_save_api_base', function () {
    if (!current_user_can('manage_woocommerce')) {
        wp_die('אין הרשאה');
    }
    check_admin_referer('pr_connect_save_api');
    $raw = isset($_POST['pr_api_base_override']) ? (string) wp_unslash($_POST['pr_api_base_override']) : '';
    $v = trim($raw);
    if ($v === '') {
        delete_option('pr_api_base_override');
        wp_safe_redirect(admin_url('admin.php?page=price-resolver-connect'));
        exit;
    }
    if (!preg_match('#^https?://#i', $v)) {
        set_transient('pr_last_error', 'כתובת API חייבת להתחיל ב-http:// או https://', 10 * MINUTE_IN_SECONDS);
        wp_safe_redirect(admin_url('admin.php?page=price-resolver-connect&pr_done=err'));
        exit;
    }
    update_option('pr_api_base_override', untrailingslashit($v));
    wp_safe_redirect(admin_url('admin.php?page=price-resolver-connect'));
    exit;
});

add_action('admin_post_pr_connect_submit', function () {
    if (!current_user_can('manage_woocommerce')) {
        wp_die('אין הרשאה');
    }
    check_admin_referer('pr_connect');
    $fail = function (string $reason) {
        set_transient('pr_last_error', $reason, 10 * MINUTE_IN_SECONDS);
        wp_safe_redirect(admin_url('admin.php?page=price-resolver-connect&pr_done=err'));
        exit;
    };
    if (!class_exists('WooCommerce') && !function_exists('WC')) {
        $fail('WooCommerce לא פעיל באתר.');
    }

    // תאימות רחבה בין גרסאות WooCommerce / אירוחים.
    if (!function_exists('pr_wc_rand_hash')) {
        function pr_wc_rand_hash() {
            if (function_exists('wc_rand_hash')) {
                return wc_rand_hash();
            }
            return wp_generate_password(32, false, false);
        }
    }
    if (!function_exists('pr_wc_api_hash')) {
        function pr_wc_api_hash($value) {
            if (function_exists('wc_api_hash')) {
                return wc_api_hash($value);
            }
            return hash('sha256', $value);
        }
    }
    if (!function_exists('pr_wc_hash_secret')) {
        function pr_wc_hash_secret($value) {
            if (function_exists('wc_hash')) {
                return wc_hash($value);
            }
            // לא משתמשים בזה ל-insert ישיר ל-DB של Woo (consumer_secret לרוב נשמר גולמי וקצר).
            return $value;
        }
    }
    if (!function_exists('pr_wc_generate_consumer_token')) {
        function pr_wc_generate_consumer_token($prefix) {
            // פורמט מקובל של Woo: ck_/cs_ + 40 תווים => אורך כולל 43.
            if (function_exists('wc_rand_hash')) {
                $h = wc_rand_hash();
            } else {
                $h = wp_generate_password(40, false, false);
            }
            $h = preg_replace('/[^a-zA-Z0-9]/', '', (string) $h);
            if (strlen($h) < 40) {
                $h = str_pad($h, 40, '0');
            }
            if (strlen($h) > 40) {
                $h = substr($h, 0, 40);
            }
            return $prefix . $h;
        }
    }
    if (!function_exists('pr_try_create_keys_via_wc_auth')) {
        function pr_try_create_keys_via_wc_auth($user_id) {
            // חלק מהגרסאות תומכות ב־WC_Auth::create_keys; ננסה כמה חתימות.
            $variants = array(
                array('Price Resolver', 'read_write', (int) $user_id),
                array((int) $user_id, 'Price Resolver', 'read_write'),
            );
            if (class_exists('WC_Auth')) {
                foreach ($variants as $args) {
                    try {
                        if (is_callable(array('WC_Auth', 'create_keys'))) {
                            $out = call_user_func_array(array('WC_Auth', 'create_keys'), $args);
                            if (is_array($out) && !empty($out['consumer_key']) && !empty($out['consumer_secret'])) {
                                return array(
                                    'consumer_key' => (string) $out['consumer_key'],
                                    'consumer_secret' => (string) $out['consumer_secret'],
                                );
                            }
                        }
                    } catch (\Throwable $e) {
                        // fallback below
                    }
                    try {
                        $inst = new WC_Auth();
                        if (is_callable(array($inst, 'create_keys'))) {
                            $out = call_user_func_array(array($inst, 'create_keys'), $args);
                            if (is_array($out) && !empty($out['consumer_key']) && !empty($out['consumer_secret'])) {
                                return array(
                                    'consumer_key' => (string) $out['consumer_key'],
                                    'consumer_secret' => (string) $out['consumer_secret'],
                                );
                            }
                        }
                    } catch (\Throwable $e) {
                        // fallback below
                    }
                }
            }
            return false;
        }
    }
    if (!function_exists('pr_try_create_keys_via_db')) {
        function pr_try_create_keys_via_db($user_id, &$err) {
            global $wpdb;
            $consumer_key = pr_wc_generate_consumer_token('ck_');
            $consumer_secret = pr_wc_generate_consumer_token('cs_');
            $table = $wpdb->prefix . 'woocommerce_api_keys';

            $exists = $wpdb->get_var($wpdb->prepare("SHOW TABLES LIKE %s", $table));
            if ($exists !== $table) {
                $err = 'טבלת WooCommerce API keys לא נמצאה: ' . $table;
                return false;
            }

            $columns = $wpdb->get_col("SHOW COLUMNS FROM `{$table}`", 0);
            if (!is_array($columns) || empty($columns)) {
                $err = 'לא ניתן לקרוא עמודות מהטבלה: ' . $table;
                return false;
            }

            $candidate = array(
                'user_id' => (int) $user_id,
                'description' => 'Price Resolver',
                'permissions' => 'read_write',
                'consumer_key' => pr_wc_api_hash($consumer_key),
                // Woo שומר consumer_secret בערך קצר (לרוב 43 תווים), לא hash ארוך.
                'consumer_secret' => $consumer_secret,
                'truncated_key' => substr($consumer_key, -7),
                'last_access' => null,
            );
            $format_map = array(
                'user_id' => '%d',
                'description' => '%s',
                'permissions' => '%s',
                'consumer_key' => '%s',
                'consumer_secret' => '%s',
                'truncated_key' => '%s',
                'last_access' => '%s',
            );
            $data = array();
            $formats = array();
            foreach ($candidate as $k => $v) {
                if (in_array($k, $columns, true)) {
                    $data[$k] = $v;
                    $formats[] = $format_map[$k];
                }
            }
            $ok = $wpdb->insert($table, $data, $formats);
            if (!$ok) {
                $err = 'DB insert נכשל (' . $table . '): ' . (string) $wpdb->last_error;
                return false;
            }
            return array(
                'consumer_key' => $consumer_key,
                'consumer_secret' => $consumer_secret,
            );
        }
    }

    $user_id = get_current_user_id();
    if (!$user_id) {
        $fail('לא ניתן לזהות משתמש מחובר בוורדפרס.');
    }
    $created = pr_try_create_keys_via_wc_auth($user_id);
    if (!$created) {
        $why = '';
        $created = pr_try_create_keys_via_db($user_id, $why);
        if (!$created) {
            $fail('נכשלה יצירת מפתח API. ' . $why);
        }
    }
    $consumer_key = (string) $created['consumer_key'];
    $consumer_secret = (string) $created['consumer_secret'];
    $site = untrailingslashit(home_url());
    $payload = array(
        'setup_token' => PR_SETUP_TOKEN,
        'site_url' => $site,
        'consumer_key' => $consumer_key,
        'consumer_secret' => $consumer_secret,
    );
    $base_from_opt = trim((string) get_option('pr_api_base_override', ''));
    $effective_base = $base_from_opt !== '' ? $base_from_opt : (defined('PR_API_BASE') ? (string) PR_API_BASE : '');
    $url = rtrim($effective_base, '/') . '/api/integrations/wordpress/connect';
    $res = wp_remote_post(
        $url,
        array(
            'timeout' => 45,
            'headers' => array('Content-Type' => 'application/json; charset=utf-8'),
            'body' => wp_json_encode($payload),
        )
    );
    if (is_wp_error($res)) {
        $fail('שגיאת רשת ב־WP: ' . $res->get_error_message() . ' | URL: ' . $url);
    }
    $code = wp_remote_retrieve_response_code($res);
    if ($code < 200 || $code >= 300) {
        $body = (string) wp_remote_retrieve_body($res);
        if (strlen($body) > 1200) {
            $body = substr($body, 0, 1200) . '...';
        }
        $fail('Backend החזיר קוד ' . $code . '. תגובה: ' . $body . ' | URL: ' . $url);
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
