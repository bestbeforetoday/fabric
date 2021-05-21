/*
Copyright IBM Corp All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gateway

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"syscall"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/gateway"
	"github.com/hyperledger/fabric-protos-go/peer"
	"github.com/hyperledger/fabric/integration/nwo"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/tedsuo/ifrit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("GatewayService", func() {
	var (
		testDir   string
		network   *nwo.Network
		org1Peer0 *nwo.Peer
		process   ifrit.Process
	)

	BeforeEach(func() {
		var err error
		testDir, err = ioutil.TempDir("", "gateway")
		Expect(err).NotTo(HaveOccurred())

		client, err := docker.NewClientFromEnv()
		Expect(err).NotTo(HaveOccurred())

		config := nwo.BasicEtcdRaft()
		network = nwo.New(config, testDir, client, StartPort(), components)

		network.GatewayEnabled = true

		network.GenerateConfigTree()
		network.Bootstrap()

		networkRunner := network.NetworkGroupRunner()
		process = ifrit.Invoke(networkRunner)
		Eventually(process.Ready(), network.EventuallyTimeout).Should(BeClosed())

		orderer := network.Orderer("orderer")
		network.CreateAndJoinChannel(orderer, "testchannel")
		network.UpdateChannelAnchors(orderer, "testchannel")
		network.VerifyMembership(
			network.PeersWithChannel("testchannel"),
			"testchannel",
		)
		nwo.EnableCapabilities(
			network,
			"testchannel",
			"Application", "V2_0",
			orderer,
			network.PeersWithChannel("testchannel")...,
		)

		chaincode := nwo.Chaincode{
			Name:            "gatewaycc",
			Version:         "0.0",
			Path:            components.Build("github.com/hyperledger/fabric/integration/chaincode/simple/cmd"),
			Lang:            "binary",
			PackageFile:     filepath.Join(testDir, "gatewaycc.tar.gz"),
			Ctor:            `{"Args":[]}`,
			SignaturePolicy: `AND ('Org1MSP.peer')`,
			Sequence:        "1",
			InitRequired:    false,
			Label:           "gatewaycc_label",
		}

		nwo.DeployChaincode(network, "testchannel", orderer, chaincode)

		org1Peer0 = network.Peer("Org1", "peer0")
	})

	AfterEach(func() {
		if process != nil {
			process.Signal(syscall.SIGTERM)
			Eventually(process.Wait(), network.EventuallyTimeout).Should(Receive())
		}
		if network != nil {
			network.Cleanup()
		}
		os.RemoveAll(testDir)
	})

	Describe("calling Evaluate", func() {
		It("should respond with the expected result", func() {
			conn := network.PeerClientConn(org1Peer0)
			defer conn.Close()
			gatewayClient := gateway.NewGatewayClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), network.EventuallyTimeout)
			defer cancel()

			signingIdentity := network.PeerUserSigner(org1Peer0, "User1")
			proposedTransaction, transactionID := NewProposedTransaction(signingIdentity, "testchannel", "gatewaycc", "respond", []byte("200"), []byte("conga message"), []byte("conga payload"))

			request := &gateway.EvaluateRequest{
				TransactionId:       transactionID,
				ChannelId:           "testchannel",
				ProposedTransaction: proposedTransaction,
			}

			response, err := gatewayClient.Evaluate(ctx, request)
			Expect(err).NotTo(HaveOccurred())
			expectedResponse := &gateway.EvaluateResponse{
				Result: &peer.Response{
					Status:  200,
					Message: "conga message",
					Payload: []byte("conga payload"),
				},
			}
			Expect(response.Result.Payload).To(Equal(expectedResponse.Result.Payload))
			Expect(proto.Equal(response, expectedResponse)).To(BeTrue(), "Expected\n\t%#v\nto proto.Equal\n\t%#v", response, expectedResponse)
		})
	})

	Describe("calling Submit", func() {
		It("should respond with the expected result", func() {
			conn := network.PeerClientConn(org1Peer0)
			defer conn.Close()
			gatewayClient := gateway.NewGatewayClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), network.EventuallyTimeout)
			defer cancel()

			signingIdentity := network.PeerUserSigner(org1Peer0, "User1")
			proposedTransaction, transactionID := NewProposedTransaction(signingIdentity, "testchannel", "gatewaycc", "respond", []byte("200"), []byte("conga message"), []byte("conga payload"))

			endorseRequest := &gateway.EndorseRequest{
				TransactionId:       transactionID,
				ChannelId:           "testchannel",
				ProposedTransaction: proposedTransaction,
			}

			endorseResponse, err := gatewayClient.Endorse(ctx, endorseRequest)
			Expect(err).NotTo(HaveOccurred())

			result := endorseResponse.GetResult()
			expectedResult := &peer.Response{
				Status:  200,
				Message: "conga message",
				Payload: []byte("conga payload"),
			}
			Expect(result.Payload).To(Equal(expectedResult.Payload))
			Expect(proto.Equal(result, expectedResult)).To(BeTrue(), "Expected\n\t%#v\nto proto.Equal\n\t%#v", result, expectedResult)

			preparedTransaction := endorseResponse.GetPreparedTransaction()
			preparedTransaction.Signature, err = signingIdentity.Sign(preparedTransaction.Payload)
			Expect(err).NotTo(HaveOccurred())

			submitRequest := &gateway.SubmitRequest{
				TransactionId:       transactionID,
				ChannelId:           "testchannel",
				PreparedTransaction: preparedTransaction,
			}
			_, err = gatewayClient.Submit(ctx, submitRequest)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("CommitStatus", func() {
		var conn *grpc.ClientConn
		var gatewayClient gateway.GatewayClient
		var ctx context.Context
		var cancel context.CancelFunc
		var signingIdentity *nwo.SigningIdentity
		var transactionID string
		var identity []byte

		BeforeEach(func() {
			conn = network.PeerClientConn(org1Peer0)
			gatewayClient = gateway.NewGatewayClient(conn)
			ctx, cancel = context.WithTimeout(context.Background(), network.EventuallyTimeout)

			signingIdentity = network.PeerUserSigner(org1Peer0, "User1")
			var proposedTransaction *peer.SignedProposal
			proposedTransaction, transactionID = NewProposedTransaction(signingIdentity, "testchannel", "gatewaycc", "respond", []byte("200"), []byte("conga message"), []byte("conga payload"))

			endorseRequest := &gateway.EndorseRequest{
				TransactionId:       transactionID,
				ChannelId:           "testchannel",
				ProposedTransaction: proposedTransaction,
			}

			endorseResponse, err := gatewayClient.Endorse(ctx, endorseRequest)
			Expect(err).NotTo(HaveOccurred())

			preparedTransaction := endorseResponse.GetPreparedTransaction()
			preparedTransaction.Signature, err = signingIdentity.Sign(preparedTransaction.Payload)
			Expect(err).NotTo(HaveOccurred())

			submitRequest := &gateway.SubmitRequest{
				TransactionId:       transactionID,
				ChannelId:           "testchannel",
				PreparedTransaction: preparedTransaction,
			}
			_, err = gatewayClient.Submit(ctx, submitRequest)
			Expect(err).NotTo(HaveOccurred())

			identity, err = signingIdentity.Serialize()
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			conn.Close()
			cancel()
		})

		It("should respond with status of submitted transaction", func() {
			statusRequest := &gateway.CommitStatusRequest{
				ChannelId:     "testchannel",
				Identity:      identity,
				TransactionId: transactionID,
			}

			statusRequestBytes, err := proto.Marshal(statusRequest)
			Expect(err).NotTo(HaveOccurred())

			signature, err := signingIdentity.Sign(statusRequestBytes)
			Expect(err).NotTo(HaveOccurred())

			signedStatusRequest := &gateway.SignedCommitStatusRequest{
				Request:   statusRequestBytes,
				Signature: signature,
			}

			status, err := gatewayClient.CommitStatus(ctx, signedStatusRequest)
			Expect(err).NotTo(HaveOccurred())

			Expect(status.Result).To(Equal(peer.TxValidationCode_VALID))
			Expect(status.BlockNumber).To(BeNumerically(">", 0))
		})

		It("should fail on unauthorized identity", func() {
			badSigningIdentity := network.OrdererUserSigner(network.Orderer("orderer"), "Admin")
			badIdentity, err := badSigningIdentity.Serialize()
			Expect(err).NotTo(HaveOccurred())

			statusRequest := &gateway.CommitStatusRequest{
				ChannelId:     "testchannel",
				Identity:      badIdentity,
				TransactionId: transactionID,
			}
			statusRequestBytes, err := proto.Marshal(statusRequest)
			Expect(err).NotTo(HaveOccurred())

			signature, err := badSigningIdentity.Sign(statusRequestBytes)
			Expect(err).NotTo(HaveOccurred())

			signedStatusRequest := &gateway.SignedCommitStatusRequest{
				Request:   statusRequestBytes,
				Signature: signature,
			}

			_, err = gatewayClient.CommitStatus(ctx, signedStatusRequest)
			Expect(err).To(HaveOccurred())

			grpcErr, _ := status.FromError(err)
			Expect(grpcErr.Code()).To(Equal(codes.PermissionDenied))
		})

		It("should fail on bad signature", func() {
			statusRequest := &gateway.CommitStatusRequest{
				ChannelId:     "testchannel",
				Identity:      identity,
				TransactionId: transactionID,
			}

			statusRequestBytes, err := proto.Marshal(statusRequest)
			Expect(err).NotTo(HaveOccurred())

			signature, err := signingIdentity.Sign([]byte("WRONG"))
			Expect(err).NotTo(HaveOccurred())

			signedStatusRequest := &gateway.SignedCommitStatusRequest{
				Request:   statusRequestBytes,
				Signature: signature,
			}

			_, err = gatewayClient.CommitStatus(ctx, signedStatusRequest)
			Expect(err).To(HaveOccurred())

			grpcErr, _ := status.FromError(err)
			Expect(grpcErr.Code()).To(Equal(codes.PermissionDenied))
		})
	})
})
