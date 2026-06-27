package event

import "testing"

func TestValidateSDKPayloadAcceptsSDKPayload(t *testing.T) {
	if err := ValidateSDKPayload([]byte(validSDKPayload)); err != nil {
		t.Fatalf("ValidateSDKPayload() error = %v", err)
	}
}

func TestValidateSDKPayloadRejectsMissingRequiredField(t *testing.T) {
	body := `{"project_id":"demo","event_id":"evt_001","user_id":"u_001","session_id":"s_001","event_time":"2026-06-27T10:00:00.000Z","properties_json":"{}"}`

	if err := ValidateSDKPayload([]byte(body)); err == nil {
		t.Fatal("ValidateSDKPayload() error = nil, want missing event_name error")
	}
}

func TestValidateSDKPayloadRejectsInvalidPropertiesJSON(t *testing.T) {
	body := `{"project_id":"demo","event_id":"evt_001","user_id":"u_001","session_id":"s_001","event_time":"2026-06-27T10:00:00.000Z","event_name":"page_view","properties_json":"[]"}`

	if err := ValidateSDKPayload([]byte(body)); err == nil {
		t.Fatal("ValidateSDKPayload() error = nil, want properties_json error")
	}
}

const validSDKPayload = `{
	"project_id":"demo-shoppingmall",
	"event_id":"evt_001",
	"user_id":"u_001",
	"session_id":"s_001",
	"event_time":"2026-06-27T10:00:00.000Z",
	"event_name":"product_view",
	"channel":"demo",
	"campaign_id":"cmp_001",
	"age_group":"30s",
	"gender":"male",
	"device":"mobile",
	"category":"Home/Eco-Friendly",
	"product_id":"GGOEGCBD142299",
	"inventory_status":"in_stock",
	"price":12900,
	"quantity":1,
	"revenue":12900,
	"coupon_id":"",
	"order_id":"",
	"experiment_id":"",
	"variant_id":"",
	"action_id":"",
	"mapping_id":"",
	"ad_id":"",
	"creative_id":"cr_001",
	"bandit_policy_id":"",
	"bandit_arm_id":"",
	"bandit_decision_id":"",
	"reward_value":0,
	"properties_json":"{\"page\":{\"path\":\"/products/sku-1\"},\"sdk\":{\"name\":\"loop-ad_event_sdk\"}}"
}`
