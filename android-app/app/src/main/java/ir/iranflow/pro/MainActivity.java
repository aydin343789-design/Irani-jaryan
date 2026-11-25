package ir.iranflow.pro;

import android.app.Activity;
import android.os.Bundle;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.webkit.WebChromeClient;
import android.webkit.WebSettings;
import android.widget.Toast;
import com.chaquo.python.Python;
import com.chaquo.python.android.AndroidPlatform;

public class MainActivity extends Activity {
    WebView web;
    @Override public void onCreate(Bundle b) {
        super.onCreate(b);
        if (!Python.isStarted()) Python.start(new AndroidPlatform(this));
        try {
            Python.getInstance().getModule("launcher").callAttr("start_server");
        } catch (Exception e) {
            Toast.makeText(this, "خطا در راه‌اندازی موتور داخلی: " + e.getMessage(), Toast.LENGTH_LONG).show();
        }
        web = new WebView(this);
        setContentView(web);
        WebSettings settings = web.getSettings();
        settings.setJavaScriptEnabled(true);
        settings.setDomStorageEnabled(true);
        web.setWebViewClient(new WebViewClient());
        web.setWebChromeClient(new WebChromeClient());
        web.loadUrl("http://127.0.0.1:8765/");
    }
    @Override public void onBackPressed() {
        if (web != null && web.canGoBack()) web.goBack(); else super.onBackPressed();
    }
}