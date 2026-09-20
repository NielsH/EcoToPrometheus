namespace EcoToPrometheus.Web
{
    using System;
    using System.Security.Cryptography;
    using System.Text;
    using Eco.Gameplay.Players;
    using Microsoft.AspNetCore.Http;
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.AspNetCore.Mvc.Filters;

    /// <summary>
    /// Config-driven access check for the metrics endpoints, evaluated per request from <c>MetricsPlugin.Obj.Config</c>.
    /// <list type="bullet">
    /// <item><c>AllowAnonymous=true</c>: every request passes, headers are ignored.</item>
    /// <item>Otherwise the request must carry <c>X-API-Key</c> or <c>?api_key=</c> equal to Users.eco <c>APIAuthToken</c> or
    /// <c>APIAdminAuthToken</c>. A missing, empty or whitespace key is refused (Eco's own handler authenticates an empty key,
    /// see Eco.Webserver/Web/Authentication/EcoApiAuthentication.cs:50), and when neither token is configured everything is
    /// refused so the endpoint is never open by accident.</item>
    /// </list>
    /// It is an attribute implementing <see cref="IAuthorizationFilter"/> directly so no DI registration is needed (a mod cannot
    /// register services). The controller also carries <c>[AllowAnonymous]</c> to keep Eco's global fallback policy
    /// (RequireAuthenticatedUser) from pre-empting this filter; MVC still runs authorization filters on such endpoints.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
    public sealed class ApiKeyOrAnonymousAttribute : Attribute, IAuthorizationFilter
    {
        public const string HeaderName = "X-API-Key";
        public const string QueryName  = "api_key";

        /// <summary>Set on <c>HttpContext.Items</c> after a successful check: "anonymous", "api-user" or "api-admin-user".</summary>
        public const string PrincipalItemKey = "EcoToPrometheus.Principal";

        public void OnAuthorization(AuthorizationFilterContext context)
        {
            try
            {
                var plugin = MetricsPlugin.Obj;
                if (plugin != null && plugin.Config.AllowAnonymous)
                {
                    context.HttpContext.Items[PrincipalItemKey] = "anonymous";
                    return;
                }

                var outcome = Check(context.HttpContext.Request, out var principal);
                if (outcome == null) { context.HttpContext.Items[PrincipalItemKey] = principal; return; }
                context.Result = Refuse(context.HttpContext, 401, outcome);
            }
            catch (Exception ex)
            {
                // Never let an auth failure surface as an unhandled exception: refuse instead.
                context.Result = Refuse(context.HttpContext, 500, $"API key check failed: {ex.GetType().Name}");
            }
        }

        /// <summary>True when Users.eco carries at least one non-empty API token. Used by the startup warning.</summary>
        public static bool AnyTokenConfigured()
        {
            try
            {
                var cfg = UserManager.Config;
                return !string.IsNullOrEmpty(cfg.APIAuthToken) || !string.IsNullOrEmpty(cfg.APIAdminAuthToken);
            }
            catch { return false; }
        }

        /// <summary>Returns null on success (with <paramref name="principal"/> set), otherwise the refusal message.</summary>
        static string? Check(HttpRequest request, out string principal)
        {
            principal = string.Empty;
            if (!TryGetPresentedKey(request, out var presented)) return "API key required";
            if (string.IsNullOrWhiteSpace(presented))             return "API key required";

            var cfg        = UserManager.Config;
            var userToken  = cfg.APIAuthToken;
            var adminToken = cfg.APIAdminAuthToken;
            if (string.IsNullOrEmpty(userToken) && string.IsNullOrEmpty(adminToken)) return "No API token configured on this server";

            if (Matches(presented, adminToken)) { principal = "api-admin-user"; return null; }
            if (Matches(presented, userToken))  { principal = "api-user";       return null; }
            return "Invalid API key";
        }

        /// <summary>Same precedence as Eco's handler: the query parameter wins over the header when both are present.</summary>
        static bool TryGetPresentedKey(HttpRequest request, out string presented)
        {
            if (request.Query.TryGetValue(QueryName, out var q))    { presented = q.ToString();  return true; }
            if (request.Headers.TryGetValue(HeaderName, out var h)) { presented = h.ToString(); return true; }
            presented = string.Empty;
            return false;
        }

        static bool Matches(string presented, string? configured)
        {
            if (string.IsNullOrEmpty(configured)) return false;
            var a = Encoding.UTF8.GetBytes(presented);
            var b = Encoding.UTF8.GetBytes(configured);
            return CryptographicOperations.FixedTimeEquals(a, b);
        }

        static ContentResult Refuse(HttpContext http, int status, string message)
        {
            if (status == 401) http.Response.Headers.WWWAuthenticate = HeaderName;
            http.Response.Headers.CacheControl = "no-store";
            return new ContentResult { StatusCode = status, Content = message, ContentType = "text/plain" };
        }
    }
}
