// pkg/kafka/consumer/consumer.go

package consumer

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Consumer representa um consumidor Kafka.
type Consumer struct {
	consumer sarama.Consumer
}

// NewConsumer cria uma instância do consumidor Kafka.
func NewConsumer(brokers []string, groupID string, topics []string) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Group.Session.Timeout = 10 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	config.Consumer.Group.Member.UserData = []byte(groupID)
	config.ClientID = groupID

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("erro ao criar consumidor: %v", err)
	}

	return &Consumer{consumer: consumer}, nil
}

// StartConsumers inicia o consumo de mensagens dos tópicos especificados.
func (c *Consumer) StartConsumers(topics []string) {
	// Canal para capturar sinais de interrupção
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// WaitGroup para esperar que todas as goroutines concluam
	var wg sync.WaitGroup

	for _, topic := range topics {
		wg.Add(1)
		go func(t string) {
			defer wg.Done()
			c.consumeTopic(t, signals, &wg)
		}(topic)
	}

	// Aguardar sinais de interrupção ou conclusão das goroutines
	select {
	case <-signals:
		// Sinal recebido, interromper a execução
		fmt.Println("Recebido sinal de interrupção. Aguardando conclusão das goroutines de consumo...")
		wg.Wait()
		fmt.Println("Goroutines de consumo concluídas. Saindo.")
	}
}

func (c *Consumer) consumeTopic(topic string, signals <-chan os.Signal, wg *sync.WaitGroup) {
	defer wg.Done()

	partitions, err := c.consumer.Partitions(topic)
	if err != nil {
		fmt.Printf("Erro ao obter partições para o tópico %s: %v\n", topic, err)
		return
	}

	for _, partition := range partitions {
		wg.Add(1)
		go c.consumePartition(topic, partition, signals, wg)
	}
}

func (c *Consumer) consumePartition(topic string, partition int32, signals <-chan os.Signal, wg *sync.WaitGroup) {
	defer wg.Done()

	partitionConsumer, err := c.consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("Erro ao criar consumidor para a partição %d do tópico %s: %v\n", partition, topic, err)
		return
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			fmt.Printf("Erro ao fechar consumidor para a partição %d do tópico %s: %v\n", partition, topic, err)
		}
	}()

	fmt.Printf("Consumindo mensagens da partição %d do tópico %s\n", partition, topic)

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			// Processar a mensagem
			fmt.Printf("Tópico: %s, Partição: %d, Offset: %d, Mensagem: %s\n", topic, msg.Partition, msg.Offset, msg.Value)

			//commitar as mensagens
		case err := <-partitionConsumer.Errors():
			// Lidar com erros
			fmt.Printf("Erro na partição %d do tópico %s: %v\n", partition, topic, err)

		case <-signals:
			// Sinal recebido, interromper o loop de consumo
			break ConsumerLoop
		}
	}
}
