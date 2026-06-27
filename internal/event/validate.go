package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-playground/validator/v10"
)

var validate = newValidator()

// SDKPayload는 loop-ad_event_sdk가 전송하는 평면 JSON 이벤트 형식입니다.
type SDKPayload struct {
	ProjectID        string  `json:"project_id" validate:"required"`
	EventID          string  `json:"event_id" validate:"required"`
	UserID           string  `json:"user_id" validate:"required"`
	SessionID        string  `json:"session_id" validate:"required"`
	EventTime        string  `json:"event_time" validate:"required,rfc3339nano"`
	EventName        string  `json:"event_name" validate:"required"`
	Channel          string  `json:"channel"`
	CampaignID       string  `json:"campaign_id"`
	AgeGroup         string  `json:"age_group"`
	Gender           string  `json:"gender"`
	Device           string  `json:"device"`
	Category         string  `json:"category"`
	ProductID        string  `json:"product_id"`
	InventoryStatus  string  `json:"inventory_status"`
	Price            float64 `json:"price"`
	Quantity         int     `json:"quantity" validate:"gte=0"`
	Revenue          float64 `json:"revenue"`
	CouponID         string  `json:"coupon_id"`
	OrderID          string  `json:"order_id"`
	ExperimentID     string  `json:"experiment_id"`
	VariantID        string  `json:"variant_id"`
	ActionID         string  `json:"action_id"`
	MappingID        string  `json:"mapping_id"`
	AdID             string  `json:"ad_id"`
	CreativeID       string  `json:"creative_id"`
	BanditPolicyID   string  `json:"bandit_policy_id"`
	BanditArmID      string  `json:"bandit_arm_id"`
	BanditDecisionID string  `json:"bandit_decision_id"`
	RewardValue      float64 `json:"reward_value"`
	PropertiesJSON   string  `json:"properties_json" validate:"required,jsonobject"`
}

// ValidateSDKPayload는 Kafka에 보낼 원문 본문을 변경하지 않고 SDK 이벤트 형식만 검증합니다.
func ValidateSDKPayload(body []byte) error {
	var payload SDKPayload
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return fmt.Errorf("event body must match SDK JSON payload: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("event body must contain one JSON object")
	}
	if err := validate.Struct(payload); err != nil {
		return fmt.Errorf("event payload is invalid: %w", err)
	}
	return nil
}

// newValidator는 SDK 이벤트 계약에 필요한 커스텀 검증 규칙을 등록합니다.
func newValidator() *validator.Validate {
	v := validator.New(validator.WithRequiredStructEnabled())
	_ = v.RegisterValidation("rfc3339nano", validateRFC3339Nano)
	_ = v.RegisterValidation("jsonobject", validateJSONObjectString)
	return v
}

// validateRFC3339Nano는 SDK가 생성한 event_time 문자열을 Go 표준 시간 파서로 검증합니다.
func validateRFC3339Nano(field validator.FieldLevel) bool {
	_, err := time.Parse(time.RFC3339Nano, field.Field().String())
	return err == nil
}

// validateJSONObjectString은 properties_json이 JSON 객체 문자열인지 검증합니다.
func validateJSONObjectString(field validator.FieldLevel) bool {
	raw := []byte(field.Field().String())
	trimmed := bytes.TrimSpace(raw)
	if !bytes.HasPrefix(trimmed, []byte("{")) || !bytes.HasSuffix(trimmed, []byte("}")) {
		return false
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	var value map[string]json.RawMessage
	if err := decoder.Decode(&value); err != nil {
		return false
	}
	return errors.Is(decoder.Decode(&struct{}{}), io.EOF)
}
