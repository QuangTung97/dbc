package schemacheck

import "github.com/QuangTung97/dbc"

type validatorConfig struct {
	tableValidateFunc func(schema dbc.SchemaInterface, table TableInfo) error

	enableValidatePrimaryKey bool
	enableValidateUniqueKey  bool
}

func newValidatorConfig(options []ValidatorOption) validatorConfig {
	conf := validatorConfig{}
	conf.tableValidateFunc = func(schema dbc.SchemaInterface, table TableInfo) error {
		return nil
	}
	for _, fn := range options {
		fn(&conf)
	}
	return conf
}

type ValidatorOption func(conf *validatorConfig)

func WithCustomTableValidateFunc(fn func(schema dbc.SchemaInterface, table TableInfo) error) ValidatorOption {
	return func(conf *validatorConfig) {
		conf.tableValidateFunc = fn
	}
}

func WithValidatePrimaryKey(enabled bool) ValidatorOption {
	return func(conf *validatorConfig) {
		conf.enableValidatePrimaryKey = enabled
	}
}

func WithValidateUniqueKey(enabled bool) ValidatorOption {
	return func(conf *validatorConfig) {
		conf.enableValidateUniqueKey = enabled
	}
}
