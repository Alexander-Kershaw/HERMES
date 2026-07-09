# Azure Permissions

## Overview

This project uses Azure RBAC (Azure Role-Based Access Control) for access to ADLS Gen2 (the data lake service dedicated to big data analytics build on top of Azure Blob Storage, it is Hadoop compatible, so it supports engines like Apache spark and Azure Databricks). This is used to ensure thorough definitions for access management for cloud resources. RBAC has 3 components:

- The security principle: who is given access to certain resources (This is something like a user, a group (containing users), service principle, or managed identity (services with an automatically managed identity)) 

- Role definition: The collection of permissions detailing permitted operations

- Scope: Specifies the boundary of resources to which the access applies.

Azure management roles such as Owner or Contributor allow resource management but do not automatically grant data plane access to blob contents.

Blob/file access requires roles such as Storage Blob Data Contributor.

## Required development permissions

I found that the local developer identity needs:

- Reader or higher for resource visibility
- Storage Blob Data Contributor on the Hermes storage account for ADLS upload/list/read/write tests

## Required platform permissions

Azure Data Factory uses a system-assigned managed identity as defined in the terraform infrastructure code.

The ADF managed identity requires:

- Storage Blob Data Contributor on the Hermes storage account

This allows ADF to read and write lakehouse data during the batch orchestration.

## Notes

I have not explicitly written role assignments and permissions for ths project. Role assignments are not currently managed in Terraform to keep the project portable.

---