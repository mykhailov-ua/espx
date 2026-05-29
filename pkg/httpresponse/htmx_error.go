package httpresponse

import (
	"bytes"
	"html/template"
	"log/slog"
	"net/http"
	"strconv"
)

var htmxErrorFragmentTemplate = template.Must(template.New("htmx_err").Parse(`
<div class="p-4 mb-4 text-sm text-red-800 rounded-lg bg-red-50 border border-red-200 dark:bg-gray-800 dark:text-red-400 dark:border-red-800" role="alert">
    <div class="flex items-center font-semibold mb-1">
        <svg class="flex-shrink-0 inline w-4 h-4 mr-2" aria-hidden="true" xmlns="http://www.w3.org/2000/svg" fill="currentColor" viewBox="0 0 20 20">
            <path d="M10 .5a9.5 9.5 0 1 0 9.5 9.5A9.51 9.5 0 0 0 10 .5ZM9.5 4a1.5 1.5 0 1 1 3 0 1.5 1.5 0 0 1-3 0Zm1.5 11.5a1 1 0 0 1-2 0v-6a1 1 0 0 1 2 0v6Z"/>
        </svg>
        <span>Error {{.Status}} ({{.Code}})</span>
    </div>
    <div>{{.Message}}</div>
</div>
`))

var fullPageErrorTemplate = template.Must(template.New("full_err").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Error {{.Status}} - {{.Message}}</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-50 flex items-center justify-center min-h-screen p-6 font-sans">
    <div class="max-w-md w-full bg-white rounded-2xl shadow-xl border border-gray-100 p-8 text-center">
        <div class="w-16 h-16 bg-red-50 rounded-full flex items-center justify-center mx-auto mb-6 text-red-500 border border-red-100">
            <svg class="w-8 h-8" fill="none" stroke="currentColor" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path>
            </svg>
        </div>
        <h1 class="text-6xl font-black text-gray-900 tracking-tight mb-2">{{.Status}}</h1>
        <h2 class="text-xl font-bold text-gray-800 mb-4">{{.Code}}</h2>
        <p class="text-gray-500 leading-relaxed mb-8">{{.Message}}</p>
        <a href="/" class="inline-flex items-center justify-center px-5 py-2.5 text-sm font-semibold text-white bg-gray-900 hover:bg-gray-800 rounded-xl transition duration-150">
            Return to Safety
        </a>
    </div>
</body>
</html>`))

type errorTemplateData struct {
	Status  int
	Code    string
	Message string
}

// Hot path branch: avoid full reload if request is from HTMX (HX-Request: true)
func HTMXError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)

	data := errorTemplateData{
		Status:  status,
		Code:    code,
		Message: message,
	}

	var buf bytes.Buffer
	if r.Header.Get("HX-Request") == "true" {
		if err := htmxErrorFragmentTemplate.Execute(&buf, data); err != nil {
			// Fallback: template execution failed
			if _, writeErr := w.Write([]byte(`<div style="color:red; font-weight:bold;">Error: ` + message + `</div>`)); writeErr != nil {
				slog.Error("failed to write htmx error fallback response", "error", writeErr)
			}
			return
		}
	} else {
		if err := fullPageErrorTemplate.Execute(&buf, data); err != nil {
			// Fallback: template execution failed
			if _, writeErr := w.Write([]byte(`<h1>Error ` + strconv.Itoa(status) + `</h1><p>` + message + `</p>`)); writeErr != nil {
				slog.Error("failed to write full page error fallback response", "error", writeErr)
			}
			return
		}
	}

	if _, err := w.Write(buf.Bytes()); err != nil {
		slog.Error("failed to write htmx error response", "error", err)
	}
}
