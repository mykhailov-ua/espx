package ads

import (
	"encoding/json"
	"hash/crc32"
	"testing"

	"espx/internal/ads/pb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ComputeCompositeHash(campaignID, userID string) uint32 {
	if campaignID == "" && userID == "" {
		return 0
	}
	key := campaignID + userID
	return crc32.ChecksumIEEE([]byte(key))
}

func TestCompositeRouting_JSONAndProtoAlignment(t *testing.T) {
	campaignUUID := uuid.New()
	userID := "user_987654321"

	jsonPayload := struct {
		CampaignID string          `json:"campaign_id"`
		UserID     string          `json:"user_id"`
		Type       string          `json:"type"`
		ClickID    string          `json:"click_id"`
		Payload    json.RawMessage `json:"payload"`
	}{
		CampaignID: campaignUUID.String(),
		UserID:     userID,
		Type:       "click",
		ClickID:    "c12345",
		Payload:    json.RawMessage(`{}`),
	}
	jsonData, err := json.Marshal(jsonPayload)
	require.NoError(t, err)

	var reqJSON TrackRequest
	err = reqJSON.UnmarshalJSON(jsonData)
	require.NoError(t, err)

	jsonCampaignIDStr := reqJSON.CampaignID.String()
	jsonUserIDStr := reqJSON.UserID
	jsonHash := ComputeCompositeHash(jsonCampaignIDStr, jsonUserIDStr)

	pbReq := pb.AdEvent{
		CampaignId: campaignUUID[:],
		EventType:  []byte("click"),
		Metadata: &pb.EventMetadata{
			ClickId: []byte("c12345"),
			UserId:  []byte(userID),
		},
	}
	protoData, err := pbReq.MarshalVT()
	require.NoError(t, err)

	var reqProto pb.AdEvent
	err = reqProto.UnmarshalVT(protoData)
	require.NoError(t, err)

	protoCampaignUUID, err := uuid.FromBytes(reqProto.CampaignId)
	require.NoError(t, err)
	protoCampaignIDStr := protoCampaignUUID.String()
	protoUserIDStr := string(reqProto.Metadata.UserId)
	protoHash := ComputeCompositeHash(protoCampaignIDStr, protoUserIDStr)

	assert.Equal(t, jsonCampaignIDStr, protoCampaignIDStr, "Campaign IDs must be identical after extraction")
	assert.Equal(t, jsonUserIDStr, protoUserIDStr, "User IDs must be identical after extraction")
	assert.Equal(t, jsonHash, protoHash, "Hashes must be perfectly aligned across JSON and Protobuf")

	t.Logf("Aligned Campaign ID: %s", jsonCampaignIDStr)
	t.Logf("Aligned User ID: %s", jsonUserIDStr)
	t.Logf("Composite Routing Hash (CRC32): %d (0x%x)", jsonHash, jsonHash)
}

func BenchmarkCompositeRouting_JSON(b *testing.B) {
	campaignUUID := uuid.New()
	userID := "user_987654321"

	jsonPayload := struct {
		CampaignID string          `json:"campaign_id"`
		UserID     string          `json:"user_id"`
		Type       string          `json:"type"`
		ClickID    string          `json:"click_id"`
		Payload    json.RawMessage `json:"payload"`
	}{
		CampaignID: campaignUUID.String(),
		UserID:     userID,
		Type:       "click",
		ClickID:    "c12345",
		Payload:    json.RawMessage(`{}`),
	}
	jsonData, _ := json.Marshal(jsonPayload)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var reqJSON TrackRequest
		_ = reqJSON.UnmarshalJSON(jsonData)
		campaignIDStr := reqJSON.CampaignID.String()
		userIDStr := reqJSON.UserID
		_ = ComputeCompositeHash(campaignIDStr, userIDStr)
	}
}

func BenchmarkCompositeRouting_Protobuf(b *testing.B) {
	campaignUUID := uuid.New()
	userID := "user_987654321"

	pbReq := pb.AdEvent{
		CampaignId: campaignUUID[:],
		EventType:  []byte("click"),
		Metadata: &pb.EventMetadata{
			ClickId: []byte("c12345"),
			UserId:  []byte(userID),
		},
	}
	protoData, _ := pbReq.MarshalVT()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var reqProto pb.AdEvent
		_ = reqProto.UnmarshalVT(protoData)

		var extractedCamp uuid.UUID
		copy(extractedCamp[:], reqProto.CampaignId)
		campaignIDStr := extractedCamp.String()

		var userIDStr string
		if reqProto.Metadata != nil {
			userIDStr = string(reqProto.Metadata.UserId)
		}

		_ = ComputeCompositeHash(campaignIDStr, userIDStr)
	}
}
