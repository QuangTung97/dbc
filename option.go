package dbc

type schemaConfig struct {
	withoutRegistering bool
}

func newSchemaConfig(options []SchemaOption) schemaConfig {
	conf := schemaConfig{}
	for _, fn := range options {
		fn(&conf)
	}
	return conf
}

type SchemaOption func(conf *schemaConfig)

func WithSchemaNoRegistering() SchemaOption {
	return func(conf *schemaConfig) {
		conf.withoutRegistering = true
	}
}
