package autoscaling

import (
	"encoding/json"
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/pricing"
	"github.com/nemosupremo/datasize"
)

var ErrInvalidCapacity = errors.New("invalid capacity")
var ErrUnknownRole = errors.New("unknown role")
var ErrInvalidAutoscalingGroup = errors.New("auto scaling group not found")

type AutoScaler interface {
	CurrentCapacity(string) (int, error)
	DesiredCapacity(string, int, datasize.ByteSize) (int, error)
	Scale(string, int) error
}

type awsAutoscalingGroup struct {
	*autoscaling.Group
	VCPU   int
	Memory datasize.ByteSize
	Region string
}

type awsAutoScaler struct {
	svc     map[string]*autoscaling.AutoScaling
	session *session.Session
	creds   *credentials.Credentials
	groups  map[string]*awsAutoscalingGroup
}

func (a *awsAutoScaler) InstanceResources(instanceType string) (int, datasize.ByteSize, error) {
	p := pricing.New(a.session, &aws.Config{
		Region: aws.String("us-east-1"),
	})

	awsProducts, err := p.GetProducts(&pricing.GetProductsInput{
		ServiceCode:   aws.String("AmazonEC2"),
		FormatVersion: aws.String("aws_v1"),
		Filters: []*pricing.Filter{
			{

				Field: aws.String("instanceType"),
				Type:  aws.String("TERM_MATCH"),
				Value: aws.String(instanceType),
			},
		},
		MaxResults: aws.Int64(1),
	})
	if err != nil {
		return 0, 0, err
	}

	type AwsProduct struct {
		Product struct {
			Attributes struct {
				VCPU   int               `json:"vcpu,string"`
				Memory datasize.ByteSize `json:"memory"`
			} `json:"attributes"`
		} `json:"product"`
	}

	var products []AwsProduct
	if b, err := json.Marshal(awsProducts.PriceList); err == nil {
		if err := json.Unmarshal(b, &products); err == nil {
			for _, product := range products {
				return product.Product.Attributes.VCPU, product.Product.Attributes.Memory, nil
			}
			return 0, 0, errors.New("invalid instance type " + instanceType)
		} else {
			return 0, 0, err
		}
	} else {
		return 0, 0, err
	}
}

func NewAwsAutoScaler(awsCredentials *credentials.Credentials, regions []string, groupIds ...string) (AutoScaler, error) {
	a := &awsAutoScaler{}

	// Configure AWS
	if s, err := session.NewSession(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: awsCredentials,
	}); err == nil {
		a.session = s
		a.creds = awsCredentials
		a.svc = make(map[string]*autoscaling.AutoScaling)

		m := make(map[string]*awsAutoscalingGroup)
		for _, region := range regions {

			_groupIds := make([]*string, len(groupIds))
			for i, gid := range groupIds {
				_groupIds[i] = aws.String(gid)
			}
			r := autoscaling.DescribeAutoScalingGroupsInput{
				AutoScalingGroupNames: _groupIds,
			}

			svc := autoscaling.New(a.session, &aws.Config{
				Region: aws.String(region),
			})
			if out, err := svc.DescribeAutoScalingGroups(&r); err == nil {

				var launchConfigs []*string
				for _, group := range out.AutoScalingGroups {
					launchConfigs = append(launchConfigs, group.LaunchConfigurationName)
					m[*group.AutoScalingGroupName] = &awsAutoscalingGroup{
						Group:  group,
						Region: region,
					}
				}
				r := autoscaling.DescribeLaunchConfigurationsInput{
					LaunchConfigurationNames: launchConfigs,
				}
				if out, err := svc.DescribeLaunchConfigurations(&r); err == nil {
					for _, lc := range out.LaunchConfigurations {
						if cpu, mem, err := a.InstanceResources(*lc.InstanceType); err == nil {
							for _, g := range m {
								if *g.LaunchConfigurationName == *lc.LaunchConfigurationName {
									g.VCPU = cpu
									g.Memory = mem
								}
							}
						} else {
							return nil, err
						}
					}
				} else {
					return nil, err
				}
			} else {
				return nil, err
			}
			a.svc[region] = svc
		}
		a.groups = m
		return a, nil
	} else {
		return nil, err
	}
}

func (a *awsAutoScaler) CurrentCapacity(role string) (int, error) {
	group, ok := a.groups[role]
	if !ok {
		return 0, ErrUnknownRole
	}

	svc := a.svc[group.Region]
	r := autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []*string{group.AutoScalingGroupName},
	}

	if out, err := svc.DescribeAutoScalingGroups(&r); err == nil {
		for _, g := range out.AutoScalingGroups {
			if *g.AutoScalingGroupName == *group.AutoScalingGroupName {
				a.groups[role].Group = g
				return int(*g.DesiredCapacity), nil
			}
		}
		return 0, ErrInvalidAutoscalingGroup
	} else {
		return 0, nil
	}
}

func (a *awsAutoScaler) DesiredCapacity(role string, cpu int, memory datasize.ByteSize) (int, error) {
	group, ok := a.groups[role]
	if !ok {
		return 0, ErrUnknownRole
	}

	desiredCapacityCPU := (cpu / group.VCPU) + 1
	desiredCapacityMem := int((memory / group.Memory) + 1)

	capacity := desiredCapacityCPU
	if capacity < desiredCapacityMem {
		capacity = desiredCapacityMem
	}

	if capacity < int(*group.MinSize) {
		capacity = int(*group.MinSize)
	} else if capacity > int(*group.MaxSize) {
		capacity = int(*group.MaxSize)
	}

	return capacity, nil
}

func (a *awsAutoScaler) Scale(role string, capacity int) error {
	group, ok := a.groups[role]
	if !ok {
		return ErrUnknownRole
	}
	svc := a.svc[group.Region]

	if capacity < int(*group.MinSize) {
		return ErrInvalidCapacity
	} else if capacity > int(*group.MaxSize) {
		return ErrInvalidCapacity
	}

	r := autoscaling.SetDesiredCapacityInput{
		AutoScalingGroupName: group.AutoScalingGroupName,
		DesiredCapacity:      aws.Int64(int64(capacity)),
	}
	_, err := svc.SetDesiredCapacity(&r)
	return err
}
