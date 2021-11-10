/*
 * Copyright 2019 IBM All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.hyperledger.fabric.gateway.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import bsp_transaction.BspTransactionOuterClass;
import com.google.common.primitives.Bytes;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hyperledger.fabric.gateway.ContractException;
import org.hyperledger.fabric.gateway.GatewayRuntimeException;
import org.hyperledger.fabric.gateway.Transaction;
import org.hyperledger.fabric.gateway.impl.query.QueryImpl;
import org.hyperledger.fabric.gateway.spi.CommitHandler;
import org.hyperledger.fabric.gateway.spi.CommitHandlerFactory;
import org.hyperledger.fabric.gateway.spi.Query;
import org.hyperledger.fabric.gateway.spi.QueryHandler;
import org.hyperledger.fabric.protos.ledger.rwset.kvrwset.KvRwset;
import org.hyperledger.fabric.protos.orderer.ClusterOuterClass;
import org.hyperledger.fabric.protos.peer.ProposalResponsePackage;
import org.hyperledger.fabric.sdk.*;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.exception.ProposalException;
import org.hyperledger.fabric.sdk.exception.ServiceDiscoveryException;


import static org.hyperledger.fabric.sdk.Channel.DiscoveryOptions.createDiscoveryOptions;

public final class TransactionImpl implements Transaction {
    private static final Log LOG = LogFactory.getLog(TransactionImpl.class);

    private static final long DEFAULT_ORDERER_TIMEOUT = 60;
    private static final TimeUnit DEFAULT_ORDERER_TIMEOUT_UNIT = TimeUnit.SECONDS;

    private final ContractImpl contract;
    private final String name;
    private final NetworkImpl network;
    private final Channel channel;
    private final GatewayImpl gateway;
    private final CommitHandlerFactory commitHandlerFactory;
    private TimePeriod commitTimeout;
    private final QueryHandler queryHandler;
    private Map<String, byte[]> transientData = null;
    private Collection<Peer> endorsingPeers = null;

//    private String corfu_host = "141.223.121.139";
//    private int corfu_port = 54323;
//
//    private ManagedChannel corfu_channel = ManagedChannelBuilder.forAddress(corfu_host, corfu_port)
//            .usePlaintext()
//            .build();
//
//     CorfuConnectGrpc.CorfuConnectBlockingStub stub =
//            CorfuConnectGrpc.newBlockingStub(corfu_channel);

    TransactionImpl(final ContractImpl contract, final String name) {
        this.contract = contract;
        this.name = name;
        network = contract.getNetwork();
        channel = network.getChannel();
        gateway = network.getGateway();
        commitHandlerFactory = gateway.getCommitHandlerFactory();
        commitTimeout = gateway.getCommitTimeout();
        queryHandler = network.getQueryHandler();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Transaction setTransient(final Map<String, byte[]> transientData) {
        this.transientData = transientData;
        return this;
    }

    @Override
    public Transaction setCommitTimeout(final long timeout, final TimeUnit timeUnit) {
        commitTimeout = new TimePeriod(timeout, timeUnit);
        return this;
    }

    @Override
    public Transaction setEndorsingPeers(final Collection<Peer> peers) {
        endorsingPeers = peers;
        return this;
    }

//    @Override
//    public byte[] submit(final String... args) throws ContractException, TimeoutException, InterruptedException {
//        Collection<ProposalResponse> proposalResponses = endorseTransaction(args);
//        Collection<ProposalResponse> validResponses = validatePeerResponses(proposalResponses);
//
//        try {
//            return commitTransaction(validResponses);
//        } catch (ContractException e) {
//            e.setProposalResponses(proposalResponses);
//            throw e;
//        }
//    }

    @Override
    public byte [] submit(final String... args) throws ContractException, TimeoutException, InterruptedException {
        BspTransactionOuterClass.SubmitResponse response = endorseTransaction(args);
//        Collection<ProposalResponse> validResponses = validatePeerResponses(proposalResponses);

//            return commitTransaction(validResponses);
            return response.getPayload().toByteArray();

    }
//    //original endorseTransaction
//    private Collection<ProposalResponse> endorseTransaction(final String... args) {
//        try {
//            TransactionProposalRequest request = newProposalRequest(args);
//            return sendTransactionProposal(request);
//        } catch (InvalidArgumentException | ProposalException | ServiceDiscoveryException e) {
//            throw new GatewayRuntimeException(e);
//        }
//    }

    //original endorseTransaction
    private BspTransactionOuterClass.SubmitResponse endorseTransaction(final String... args) {
        try {
            TransactionProposalRequest request = newProposalRequest(args);
            return sendTransactionProposaltoCorfu(request);
        } catch (InvalidArgumentException | ProposalException e) {
            throw new GatewayRuntimeException(e);
        }
    }

    //vCorfu sendTransactionProposal
    //send proposal to only one peer
    private Collection<ProposalResponse> sendTransactionProposal(final TransactionProposalRequest request)
            throws ProposalException, InvalidArgumentException, ServiceDiscoveryException {
        if (endorsingPeers != null) {
            //하나의 peer에게 proposal 전송하도록!
            return channel.sendTransactionProposal(request, Collections.singleton(endorsingPeers.iterator().next()));
        } else if (network.getGateway().isDiscoveryEnabled()) {
            Channel.DiscoveryOptions discoveryOptions = createDiscoveryOptions()
                    .setEndorsementSelector(ServiceDiscovery.EndorsementSelector.ENDORSEMENT_SELECTION_RANDOM)
                    .setInspectResults(true)
                    .setForceDiscovery(true);
            return channel.sendTransactionProposalToEndorsers(request, discoveryOptions);
        } else {
            return channel.sendTransactionProposal(request);
        }
    }

    //sendTransactionProposal to vCorfu
    //send proposal to adapter module
    private BspTransactionOuterClass.SubmitResponse sendTransactionProposaltoCorfu(final TransactionProposalRequest request) throws InvalidArgumentException, ProposalException {

            //adapter moudle 전용 sendTPC 호출!
            return channel.sendTransactionProposaltoCorfu(request);


    }
// original commitTransaction
//    private byte[] commitTransaction(final Collection<ProposalResponse> validResponses)
//            throws TimeoutException, ContractException, InterruptedException {
//        ProposalResponse proposalResponse = validResponses.iterator().next();
//        String transactionId = proposalResponse.getTransactionID();
//
//        CommitHandler commitHandler = commitHandlerFactory.create(transactionId, network);
//        commitHandler.startListening();
//
//        try {
//            Channel.TransactionOptions transactionOptions = Channel.TransactionOptions.createTransactionOptions()
//                    .nOfEvents(Channel.NOfEvents.createNoEvents()); // Disable default commit wait behaviour
//            channel.sendTransaction(validResponses, transactionOptions)
//                    .get(DEFAULT_ORDERER_TIMEOUT, DEFAULT_ORDERER_TIMEOUT_UNIT);
//        } catch (TimeoutException e) {
//            commitHandler.cancelListening();
//            throw e;
//        } catch (Exception e) {
//            commitHandler.cancelListening();
//            throw new ContractException("Failed to send transaction to the orderer", e);
//        }
//
//        commitHandler.waitForEvents(commitTimeout.getTime(), commitTimeout.getTimeUnit());
//
//        try {
//            return proposalResponse.getChaincodeActionResponsePayload();
//        } catch (InvalidArgumentException e) {
//            throw new GatewayRuntimeException(e);
//        }
//    }


//version 1 for sharedlog
//private byte[] commitTransaction(final Collection<ProposalResponse> validResponses)
//        throws ContractException {
//    boolean result;
//    ProposalResponse proposalResponse = validResponses.iterator().next();
//    String transactionId = proposalResponse.getTransactionID();
//
////    CommitHandler commitHandler = commitHandlerFactory.create(transactionId, network);
////    commitHandler.startListening();
//
//    List<String> readsetkeys = new ArrayList<String>();
//    List<String> writesetkeys = new ArrayList<String>();
//
//
////    List<byte[]> readsetkeys = Arrays.asList();
////    List<byte[]> writesetkeys= Arrays.asList();
////    byte[] res = Bytes.toArray(readsetkeys.stream()
////            .map(byteArray -> Bytes.asList(byteArray))
////            .flatMap(listArray -> listArray.stream())
////            .collect(Collectors.toList()));
////    res.
//
//    try {
//        int readCount = proposalResponse.getChaincodeActionResponseReadWriteSetInfo()
//                .getNsRwsetInfo(1)
//                .getRwset()
//                .getReadsCount() - 1;
//
//        int writeCount = proposalResponse.getChaincodeActionResponseReadWriteSetInfo()
//                .getNsRwsetInfo(1)
//                .getRwset()
//                .getWritesCount();
//
//        for (int i = 1; i <= readCount; i++) {
//            String tempkey = proposalResponse.getChaincodeActionResponseReadWriteSetInfo()
//                    .getNsRwsetInfo(1)
//                    .getRwset()
//                    .getReads(i)
//                    .getKey();
//            readsetkeys.add(tempkey);
//        }
//
//        for (int j = 0; j < writeCount; j++) {
//            String tempkey1 = proposalResponse.getChaincodeActionResponseReadWriteSetInfo()
//                    .getNsRwsetInfo(1)
//                    .getRwset()
//                    .getWrites(j)
//                    .getKey();
//            writesetkeys.add(tempkey1);
//        }
//
//        ByteArrayOutputStream readsetbaos = new ByteArrayOutputStream();
//        DataOutputStream out1 = new DataOutputStream(readsetbaos);
//        for (String element : readsetkeys) {
//            out1.writeUTF(element);
//        }
//
//        ByteArrayOutputStream writesetbaos = new ByteArrayOutputStream();
//        DataOutputStream out2 = new DataOutputStream(writesetbaos);
//        for (String element : readsetkeys) {
//            out2.writeUTF(element);
//        }
//
//        byte[] readsetbytes = readsetbaos.toByteArray();
//        byte[] writesetbytes = writesetbaos.toByteArray();
//
//        ByteString translatedReadsetKeys = ByteString.copyFrom(readsetkeys.toString().getBytes());
//        ByteString translatedWritesetKeys = ByteString.copyFrom(writesetkeys.toString().getBytes());
//
//        Res_Append appendResponse = stub.sendTransaction(Req_Append.newBuilder()
//                .setChaincodeID(contract.getChaincodeId())
//                .setChannelID(channel.getName())
//                .setTxContext(proposalResponse.getProposalResponse().toByteString())
//                .setReadset(translatedReadsetKeys)
//                .setWriteset(translatedWritesetKeys)
//                .build());
//        corfu_channel.shutdown();
//
//        result = appendResponse.getSuccess();
//
//    } catch (Exception e) {
////        commitHandler.cancelListening();
//        throw new ContractException("Failed to send transaction to the vCorfu server", e);
//    }
//
//        try {
//            return proposalResponse.getChaincodeActionResponsePayload();
//
//        } catch (InvalidArgumentException e) {
//            throw new GatewayRuntimeException(e);
//        }


        //    private byte[] commitTransaction(final Collection<ProposalResponse> validResponses) {
//        ProposalResponse proposalResponse = validResponses.iterator().next();
//        String transactionId = proposalResponse.getTransactionID();
//
//        CommitHandler commitHandler = commitHandlerFactory.create(transactionId, network);
//        commitHandler.startListening();
//
//
////            Res_Append appendResponse = stub.sendTransaction(org.sslab.adapter.ProposalResponse.newBuilder()
////                    .setPayload(ByteString.copyFrom(proposalResponse.getChaincodeActionResponsePayload()))
////                    .build());
//            System.out.println("전송 성공!");
//
//
////    commitHandler.waitForEvents(commitTimeout.getTime(), commitTimeout.getTimeUnit());
//        String successmsg = "성공했습니다.";
//        byte[] bytearray = successmsg.getBytes();
//            return bytearray;
//    }
//}


    //version 2 for sharedlog
private byte[] commitTransaction(final Collection<ProposalResponse> validResponses)
        throws ContractException {
    boolean result;
    ProposalResponse proposalResponse = validResponses.iterator().next();
    String transactionId = proposalResponse.getTransactionID();

//    CommitHandler commitHandler = commitHandlerFactory.create(transactionId, network);
//    commitHandler.startListening();

//    try {
//        ResAppend appendResponse = stub.sendTransaction(ReqAppend.newBuilder()
//                .setChaincodeID(contract.getChaincodeId())
//                .setChannelID(channel.getName())
//                .setTxContext(proposalResponse.getProposalResponse().toByteString())
//                .build());
//        corfu_channel.shutdown();
//
//        result = appendResponse.getSuccess();
//    }catch (Exception e) {
//
//        throw new ContractException("Failed to send transaction to the orderer", e);
//    }
    try {
        proposalResponse.getProposal();
        return proposalResponse.getChaincodeActionResponsePayload();

    } catch (InvalidArgumentException e) {
        throw new GatewayRuntimeException(e);
    }
}

    public byte[] sendTransaction(final Collection<ProposalResponse> validResponses)
            throws TimeoutException, ContractException, InterruptedException {
        ProposalResponse proposalResponse = validResponses.iterator().next();
        String transactionId = proposalResponse.getTransactionID();

        CommitHandler commitHandler = commitHandlerFactory.create(transactionId, network);
        commitHandler.startListening();

        try {
            Channel.TransactionOptions transactionOptions = Channel.TransactionOptions.createTransactionOptions()
                    .nOfEvents(Channel.NOfEvents.createNoEvents()); // Disable default commit wait behaviour
            channel.sendTransaction(validResponses, transactionOptions)
                    .get(DEFAULT_ORDERER_TIMEOUT, DEFAULT_ORDERER_TIMEOUT_UNIT);
        } catch (TimeoutException e) {
            commitHandler.cancelListening();
            throw e;
        } catch (Exception e) {
            commitHandler.cancelListening();
            throw new ContractException("Failed to send transaction to the orderer", e);
        }

        commitHandler.waitForEvents(commitTimeout.getTime(), commitTimeout.getTimeUnit());

        try {
            return proposalResponse.getChaincodeActionResponsePayload();
        } catch (InvalidArgumentException e) {
            throw new GatewayRuntimeException(e);
        }
    }

    /* Original sendTransaction
    */

//    public byte[] sendTransaction(final Collection<ProposalResponse> validResponses)
//            throws TimeoutException, ContractException, InterruptedException {
//        ProposalResponse proposalResponse = validResponses.iterator().next();
//        String transactionId = proposalResponse.getTransactionID();
//
//        CommitHandler commitHandler = commitHandlerFactory.create(transactionId, network);
//        commitHandler.startListening();
//
//        try {
//            Channel.TransactionOptions transactionOptions = Channel.TransactionOptions.createTransactionOptions()
//                    .nOfEvents(Channel.NOfEvents.createNoEvents()); // Disable default commit wait behaviour
//            channel.sendTransaction(validResponses, transactionOptions)
//                    .get(DEFAULT_ORDERER_TIMEOUT, DEFAULT_ORDERER_TIMEOUT_UNIT);
//        } catch (TimeoutException e) {
//            commitHandler.cancelListening();
//            throw e;
//        } catch (Exception e) {
//            commitHandler.cancelListening();
//            throw new ContractException("Failed to send transaction to the orderer", e);
//        }
//
//        commitHandler.waitForEvents(commitTimeout.getTime(), commitTimeout.getTimeUnit());
//
//        try {
//            return proposalResponse.getChaincodeActionResponsePayload();
//        } catch (InvalidArgumentException e) {
//            throw new GatewayRuntimeException(e);
//        }
//    }


    private TransactionProposalRequest newProposalRequest(final String... args) {
        TransactionProposalRequest request = network.getGateway().getClient().newTransactionProposalRequest();
        configureRequest(request, args);
        if (transientData != null) {
            try {
                request.setTransientMap(transientData);
            } catch (InvalidArgumentException e) {
                // Only happens if transientData is null
                throw new IllegalStateException(e);
            }
        }
        return request;
    }

    private void configureRequest(final TransactionRequest request, final String... args) {
        request.setChaincodeName(contract.getChaincodeId());
        request.setFcn(name);
        request.setArgs(args);
    }

    private Collection<ProposalResponse> validatePeerResponses(final Collection<ProposalResponse> proposalResponses)
            throws ContractException {
        final Collection<ProposalResponse> validResponses = new ArrayList<>();
        final Collection<String> invalidResponseMsgs = new ArrayList<>();
        proposalResponses.forEach(response -> {
            String peerUrl = response.getPeer() != null ? response.getPeer().getUrl() : "<unknown>";
            if (response.getStatus().equals(ChaincodeResponse.Status.SUCCESS)) {
                LOG.debug(String.format("validatePeerResponses: valid response from peer %s", peerUrl));
                validResponses.add(response);
            } else {
                LOG.warn(String.format("validatePeerResponses: invalid response from peer %s, message %s", peerUrl, response.getMessage()));
                invalidResponseMsgs.add(response.getMessage());
            }
        });

        if (validResponses.size() < 1) {
            String msg = String.format("No valid proposal responses received. %d peer error responses: %s",
                    invalidResponseMsgs.size(), String.join("; ", invalidResponseMsgs));
            LOG.error(msg);
            throw new ContractException(msg, proposalResponses);
        }

        return validResponses;
    }

    @Override
    public byte[] evaluate(final String... args) throws ContractException {
        QueryByChaincodeRequest request = newQueryRequest(args);
        Query query = new QueryImpl(network.getChannel(), request);

        BspTransactionOuterClass.SubmitResponse response = queryHandler.evaluatetoCorfu(query);

        byte[] byteArray = response.getPayload().toByteArray();
            return byteArray;

    }

//    @Override
//    public byte[] evaluate(final String... args) throws ContractException {
//        QueryByChaincodeRequest request = newQueryRequest(args);
//        Query query = new QueryImpl(network.getChannel(), request);
//
//        String response = queryHandler.evaluatetoCorfu(query);
////        try {
////            int kvrwSet1 = response.getChaincodeActionResponseReadWriteSetInfo()
////                    .getNsRwsetInfo(1)
////                    .getRwset()
////                    .getRangeQueriesInfoCount();
//////            KvRwset.KVRead kvrwSet2 =  response.getChaincodeActionResponseReadWriteSetInfo()
//////                    .getNsRwsetInfo(1)
//////                    .getRwset()
//////                    .getReads(2);
////
//////            System.out.println(kvrwSet1);
//////            System.out.println(kvrwSet2);
////
////        } catch (InvalidArgumentException e) {
////            e.printStackTrace();
////        } catch (InvalidProtocolBufferException e) {
////            e.printStackTrace();
////        }
//
//        try {
//            return response.getChaincodeActionResponsePayload();
//        } catch (InvalidArgumentException e) {
//            throw new ContractException(response.getMessage(), e);
//        }
//    }

    private QueryByChaincodeRequest newQueryRequest(final String... args) {
        QueryByChaincodeRequest request = gateway.getClient().newQueryProposalRequest();
        configureRequest(request, args);
        if (transientData != null) {
            try {
                request.setTransientMap(transientData);
            } catch (InvalidArgumentException e) {
                // Only happens if transientData is null
                throw new IllegalStateException(e);
            }
        }
        return request;
    }
}

