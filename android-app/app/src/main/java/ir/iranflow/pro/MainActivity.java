package ir.iranflow.pro;
import android.app.Activity;
import android.os.Bundle;
import android.webkit.WebView;
import android.webkit.WebViewClient;
import android.webkit.WebChromeClient;
import android.webkit.WebSettings;
import android.widget.Toast;
import android.util.Log;
import com.chaquo.python.Python;
import com.chaquo.python.android.AndroidPlatform;
import com.chaquo.python.PyObject;
public class MainActivity extends Activity {
    WebView web;
    private void showError(String message) {
        Log.e("IranFlowBackend", message);
        web.loadDataWithBaseURL(null, "<html dir='rtl'><meta charset='utf-8'><body style='background:#101827;color:white;font-family:sans-serif;padding:24px'><h2>راه‌اندازی ناموفق بک‌اند</h2><pre style='white-space:pre-wrap;direction:ltr'>" + message.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;") + "</pre><p>برای ثبت جزئیات بیشتر این خطا در نسخه بعدی اصلاح خواهد شد.</p></body></html>", "text/html", "UTF-8", null);
        Toast.makeText(this, "خطا در راه‌اندازی بک‌اند داخلی", Toast.LENGTH_LONG).show();
    }
    @Override public void onCreate(Bundle b) {
        super.onCreate(b);
        web = new WebView(this); setContentView(web);
        WebSettings s=web.getSettings(); s.setJavaScriptEnabled(true); s.setDomStorageEnabled(true);
        web.setWebViewClient(new WebViewClient()); web.setWebChromeClient(new WebChromeClient());
        try {
            if (!Python.isStarted()) Python.start(new AndroidPlatform(this));
            PyObject result = Python.getInstance().getModule("launcher").callAttr("start_server");
            String status = result == null ? "" : result.toString();
            if (status.startsWith("ERROR:")) { showError(status); return; }
            web.postDelayed(() -> web.loadUrl("http://127.0.0.1:8765/"), 500);
        } catch (Throwable e) {
            Log.e("IranFlowBackend", "Backend launch exception", e);
            showError(Log.getStackTraceString(e));
        }
    }
    @Override public void onBackPressed(){ if(web!=null&&web.canGoBack())web.goBack();else super.onBackPressed(); }
}