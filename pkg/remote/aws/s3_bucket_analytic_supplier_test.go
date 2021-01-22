package aws

import (
	"context"
	"testing"

	remoteerror "github.com/cloudskiff/driftctl/pkg/remote/error"

	resourceaws "github.com/cloudskiff/driftctl/pkg/resource/aws"
	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/cloudskiff/driftctl/pkg/parallel"

	awsdeserializer "github.com/cloudskiff/driftctl/pkg/resource/aws/deserializer"

	"github.com/cloudskiff/driftctl/test/goldenfile"

	"github.com/cloudskiff/driftctl/pkg/resource"
	"github.com/cloudskiff/driftctl/pkg/terraform"
	"github.com/cloudskiff/driftctl/test"
	"github.com/cloudskiff/driftctl/test/mocks"
)

func TestS3BucketAnalyticSupplier_Resources(t *testing.T) {

	tests := []struct {
		test           string
		dirName        string
		bucketsIDs     []string
		bucketLocation map[string]string
		analyticsIDs   map[string][]string
		listError      error
		wantErr        error
	}{
		{
			test: "multiple bucket with multiple analytics", dirName: "s3_bucket_analytics_multiple",
			bucketsIDs: []string{
				"bucket-martin-test-drift",
				"bucket-martin-test-drift2",
				"bucket-martin-test-drift3",
			},
			bucketLocation: map[string]string{
				"bucket-martin-test-drift":  "eu-west-1",
				"bucket-martin-test-drift2": "eu-west-3",
				"bucket-martin-test-drift3": "ap-northeast-1",
			},
			analyticsIDs: map[string][]string{
				"bucket-martin-test-drift": {
					"Analytics_Bucket1",
					"Analytics2_Bucket1",
				},
				"bucket-martin-test-drift2": {
					"Analytics_Bucket2",
					"Analytics2_Bucket2",
				},
				"bucket-martin-test-drift3": {
					"Analytics_Bucket3",
					"Analytics2_Bucket3",
				},
			},
		},

		{
			test: "cannot list bucket", dirName: "s3_bucket_analytics_list_bucket",
			bucketsIDs: nil,
			listError:  awserr.NewRequestFailure(nil, 403, ""),
			bucketLocation: map[string]string{
				"bucket-martin-test-drift":  "eu-west-1",
				"bucket-martin-test-drift2": "eu-west-3",
				"bucket-martin-test-drift3": "ap-northeast-1",
			},
			analyticsIDs: map[string][]string{
				"bucket-martin-test-drift": {
					"Analytics_Bucket1",
					"Analytics2_Bucket1",
				},
				"bucket-martin-test-drift2": {
					"Analytics_Bucket2",
					"Analytics2_Bucket2",
				},
				"bucket-martin-test-drift3": {
					"Analytics_Bucket3",
					"Analytics2_Bucket3",
				},
			},
			wantErr: remoteerror.NewResourceEnumerationErrorWithType(awserr.NewRequestFailure(nil, 403, ""), resourceaws.AwsS3BucketAnalyticsConfigurationResourceType, resourceaws.AwsS3BucketResourceType),
		},
		{
			test: "cannot list Analytics", dirName: "s3_bucket_analytics_list_analytics",
			bucketsIDs: []string{
				"bucket-martin-test-drift",
				"bucket-martin-test-drift2",
				"bucket-martin-test-drift3",
			},
			bucketLocation: map[string]string{
				"bucket-martin-test-drift":  "eu-west-1",
				"bucket-martin-test-drift2": "eu-west-3",
				"bucket-martin-test-drift3": "ap-northeast-1",
			},
			analyticsIDs: nil,
			listError:    awserr.NewRequestFailure(nil, 403, ""),
			wantErr:      remoteerror.NewResourceEnumerationError(awserr.NewRequestFailure(nil, 403, ""), resourceaws.AwsS3BucketAnalyticsConfigurationResourceType),
		},
	}
	for _, tt := range tests {
		shouldUpdate := tt.dirName == *goldenfile.Update

		providerLibrary := terraform.NewProviderLibrary()
		supplierLibrary := resource.NewSupplierLibrary()

		if shouldUpdate {
			provider, err := NewTerraFormProvider()
			if err != nil {
				t.Fatal(err)
			}

			factory := AwsClientFactory{config: provider.session}
			providerLibrary.AddProvider(terraform.AWS, provider)
			supplierLibrary.AddSupplier(NewS3BucketAnalyticSupplier(provider, factory))
		}

		t.Run(tt.test, func(t *testing.T) {

			mock := mocks.NewMockAWSS3Client(tt.bucketsIDs, tt.analyticsIDs, nil, nil, tt.bucketLocation, tt.listError)

			provider := mocks.NewMockedGoldenTFProvider(tt.dirName, providerLibrary.Provider(terraform.AWS), shouldUpdate)
			factory := mocks.NewMockAwsClientFactory(mock)

			deserializer := awsdeserializer.NewS3BucketAnalyticDeserializer()
			s := &S3BucketAnalyticSupplier{
				provider,
				deserializer,
				factory,
				terraform.NewParallelResourceReader(parallel.NewParallelRunner(context.TODO(), 10)),
			}
			got, err := s.Resources()
			assert.Equal(t, err, tt.wantErr)

			test.CtyTestDiff(got, tt.dirName, provider, deserializer, shouldUpdate, t)
		})
	}
}
