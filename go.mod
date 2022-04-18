module github.com/outofforest/zbackup

go 1.18

replace github.com/ridge/parallel => github.com/outofforest/parallel v0.1.2

require (
	github.com/go-piv/piv-go v1.9.0
	github.com/outofforest/build v1.7.10
	github.com/outofforest/buildgo v0.3.5
	github.com/outofforest/ioc/v2 v2.5.0
	github.com/outofforest/libexec v0.2.1
	github.com/outofforest/run v0.2.2
	golang.org/x/term v0.0.0-20210927222741-03fcf44c2211
)

require golang.org/x/sys v0.0.0-20220128215802-99c3d69c2c27 // indirect
