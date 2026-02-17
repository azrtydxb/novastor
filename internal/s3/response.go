package s3

import (
	"encoding/xml"
	"net/http"
)

// s3Error represents an S3 XML error response.
type s3Error struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// writeXMLResponse writes an XML-encoded response with the given status code.
func writeXMLResponse(w http.ResponseWriter, statusCode int, v interface{}) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(statusCode)
	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	_ = enc.Encode(v)
}

// writeS3Error writes an S3-style XML error response.
func writeS3Error(w http.ResponseWriter, code string, message string, httpStatus int) {
	writeXMLResponse(w, httpStatus, &s3Error{
		Code:    code,
		Message: message,
	})
}
