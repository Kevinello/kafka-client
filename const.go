package kafka

// Mechanism SASL authentication mechanism constants
const (
	// MechanismSASLPlain SASL PLAINTEXT authentication
	//	@update 2024-01-03 06:54:55
	MechanismSASLPlain = "SASL_PLAINTEXT"

	// MechanismSASLSCRAMSHA256 SASL SCRAM-SHA-256 authentication
	//	@update 2024-01-03 06:55:11
	MechanismSASLSCRAMSHA256 = "SASL_SCRAM_SHA_256"

	// MechanismSASLSCRAMSHA512 SASL SCRAM-SHA-512 authentication
	//	@update 2024-01-03 06:55:17
	MechanismSASLSCRAMSHA512 = "SASL_SCRAM_SHA_512"
)
