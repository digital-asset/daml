// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.pipeline.samples.helloworldtls;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.asn1.x509.*;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.security.cert.X509Certificate;
import java.io.IOException;
import java.math.BigInteger;
import java.security.*;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Objects;

final class AuthenticationBundle {

    private final static SecureRandom random = new SecureRandom();

    public final X509Certificate trustCertCollection;

    public final PrivateKey clientPrivateKey;
    public final X509Certificate clientCertChain;

    public final PrivateKey serverPrivateKey;
    public final X509Certificate serverCertChain;

    private KeyPair generateKeyPair(String algorithm) throws NoSuchAlgorithmException {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(algorithm);
        keyPairGenerator.initialize(4096, random);
        return keyPairGenerator.generateKeyPair();
    }

    private X500Name createSubject(String commonName) {
        return new X500NameBuilder(BCStyle.INSTANCE).addRDN(BCStyle.CN, commonName).build();
    }

    private X509v3CertificateBuilder prepareCertificate(X500Name issuer, X500Name subject, PublicKey publicKey, Date validFrom, Date validUntil) {
        return new JcaX509v3CertificateBuilder(
                issuer,
                new BigInteger(160, random),
                validFrom,
                validUntil,
                subject,
                publicKey);
    }

    public AuthenticationBundle(String authorityCommonName, String serverCommonName, String clientCommonName) throws NoSuchAlgorithmException, IOException, OperatorCreationException, CertificateException {

        Instant invokeTime = Instant.now();
        Date validFrom = Date.from(invokeTime);
        Date validUntil = Date.from(invokeTime.plus(Duration.ofHours(1)));

        JcaX509CertificateConverter certificateConverter = new JcaX509CertificateConverter();
        Provider provider = new BouncyCastleProvider();
        certificateConverter.setProvider(provider);

        // create CA key pair and create a content signer using the its private key
        KeyPair authorityKeyPair = generateKeyPair("RSA");
        ContentSigner authorityContentSigner = new JcaContentSignerBuilder("SHA256withRSA").build(authorityKeyPair.getPrivate());

        // fill in CA certificate fields
        X500Name authoritySubject = createSubject(authorityCommonName);
        X509v3CertificateBuilder authorityCertificateBuilder = prepareCertificate(authoritySubject, authoritySubject, authorityKeyPair.getPublic(), validFrom, validUntil);
        BasicConstraints constraints = new BasicConstraints(true);
        authorityCertificateBuilder.addExtension(Extension.basicConstraints, true, constraints.getEncoded());

        // build CA certificate
        X509CertificateHolder authorityCertificateHolder = authorityCertificateBuilder.build(authorityContentSigner);

        this.trustCertCollection = certificateConverter.getCertificate(authorityCertificateHolder);

        // create server keypair
        KeyPair serverKeyPair = generateKeyPair("RSA");
        X500Name serverSubject = createSubject(serverCommonName);
        X509v3CertificateBuilder serverCertBuilder = prepareCertificate(authoritySubject, serverSubject, serverKeyPair.getPublic(), validFrom, validUntil);

        // build server certificate
        this.serverCertChain = certificateConverter.getCertificate(serverCertBuilder.build(authorityContentSigner));
        this.serverPrivateKey = serverKeyPair.getPrivate();

        // create client keypair
        KeyPair clientKeyPair = generateKeyPair("RSA");
        X500Name clientSubject = createSubject(clientCommonName);
        X509v3CertificateBuilder clientCertBuilder = prepareCertificate(authoritySubject, clientSubject, clientKeyPair.getPublic(), validFrom, validUntil);

        // build server certificate
        this.clientCertChain = certificateConverter.getCertificate(clientCertBuilder.build(authorityContentSigner));
        this.clientPrivateKey = clientKeyPair.getPrivate();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AuthenticationBundle that = (AuthenticationBundle) o;
        return Objects.equals(trustCertCollection, that.trustCertCollection) &&
                Objects.equals(clientPrivateKey, that.clientPrivateKey) &&
                Objects.equals(clientCertChain, that.clientCertChain) &&
                Objects.equals(serverPrivateKey, that.serverPrivateKey) &&
                Objects.equals(serverCertChain, that.serverCertChain);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trustCertCollection, clientPrivateKey, clientCertChain, serverPrivateKey, serverCertChain);
    }

    @Override
    public String toString() {
        return "AuthenticationBundle{" +
                "trustCertCollection=" + trustCertCollection +
                ", clientPrivateKey=" + clientPrivateKey +
                ", clientCertChain=" + clientCertChain +
                ", serverPrivateKey=" + serverPrivateKey +
                ", serverCertChain=" + serverCertChain +
                '}';
    }

}
