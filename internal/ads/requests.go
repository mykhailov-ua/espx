package ads

import (
	"encoding/json"
	"github.com/google/uuid"
	jlexer "github.com/mailru/easyjson/jlexer"
)

//easyjson:skip
type TrackRequest struct {
	CampaignID uuid.UUID       `json:"campaign_id"`
	UserID     string          `json:"user_id"`
	Type       string          `json:"type"`
	ClickID    string          `json:"click_id"`
	Payload    json.RawMessage `json:"payload"`
}

func (v *TrackRequest) UnmarshalEasyJSON(in *jlexer.Lexer) {
	if in.IsNull() {
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeFieldName(false)
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "campaign_id":
			if data := in.UnsafeBytes(); in.Ok() {
				_ = v.CampaignID.UnmarshalText(data)
			}
		case "user_id":
			v.UserID = in.UnsafeString()
		case "type":
			v.Type = in.UnsafeString()
		case "click_id":
			v.ClickID = in.UnsafeString()
		case "payload":
			v.Payload = in.Raw()
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
}

func (v *TrackRequest) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	v.UnmarshalEasyJSON(&r)
	return r.Error()
}
