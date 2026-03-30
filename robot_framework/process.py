"""This module contains the main process of the robot."""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement

import json
import re
from datetime import datetime

import pyodbc
import requests


# pylint: disable-next=unused-argument
def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    payload = json.loads(queue_element.data)
    filarkiv_info = json.loads(orchestrator_connection.get_constant("XFlowFilarkivValues").value)

    # Normalize the 2 supported Xflow payload formats
    if "XflowId" in payload:
        process_public_id = str(payload["XflowId"]).strip()
        form_id = payload["FormId"]
        case_title = str(payload["CaseTitle"]).strip()
        form_title = str(payload["FormTitle"]).strip()
        archive = str(payload["Archive"]).strip()
        address1 = str(payload["Address"]).strip()
        address2 = str(payload.get("AdditionalAddress") or "").strip()
        test = str(payload.get("Test") or "").strip()
    else:
        process_public_id = str(payload["xflowArbejdsgangId"]).strip()
        form_id = payload["xflowBlanketId"]
        case_title = str(payload["beskrivelse"]).strip()
        form_title = str(payload["blankettitel"]).strip()
        archive = str(payload["arkiv"]).strip()

        adresse = str(payload.get("adresse") or "").strip()
        vejnavn = str(payload.get("vejnavn") or "").strip()
        husnummer = str(payload.get("husnummer") or "").strip()

        test = str(payload.get("Test") or "").strip()

        if adresse:
            address1 = adresse
        else:
            address1 = f"{vejnavn} {husnummer}".strip()

        address2 = ""

    use_test= False
    if test.lower() == 'test':
        use_test = True
    orchestrator_connection.log_info(
        f"Start ProcessPublicId={process_public_id}, FormId={form_id}, Archive={archive}"
    )

    sql_server = orchestrator_connection.get_constant("SqlServer").value
    conn_string = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={sql_server};"
        "DATABASE=XFlowToFilarkiv;"
        "Trusted_Connection=yes;"
    )

    conn = pyodbc.connect(conn_string)
    conn.autocommit = False
    cursor = conn.cursor()

    # Ensure Xflow.Processes exists
    cursor.execute(
        """
        SELECT TOP 1 Id, Date, FilArkivCaseId
        FROM [Xflow].[Processes]
        WHERE ProcessPublicId = ?
        """,
        process_public_id,
    )
    row = cursor.fetchone()

    if row is None:
        cursor.execute(
            """
            INSERT INTO [Xflow].[Processes]
                ([AddedAt], [ProcessId], [ProcessPublicId], [CaseTitle], [Archive], [FormTitle], [UseFilArkivTestEnvironment])
            VALUES
                (SYSUTCDATETIME(), ?, ?, ?, ?, ?, ?)
            """,
            int(form_id),
            process_public_id,
            case_title,
            archive,
            form_title,
            1 if use_test else 0,
        )
        cursor.execute("SELECT CAST(SCOPE_IDENTITY() AS int)")
        process_db_id = cursor.fetchone()[0]
        process_date = None
        filarkiv_case_id = None
        orchestrator_connection.log_info(f"Inserted Xflow.Processes Id={process_db_id}")
    else:
        process_db_id = row[0]
        process_date = row[1]
        filarkiv_case_id = row[2]
        orchestrator_connection.log_info(f"Found existing Xflow.Processes Id={process_db_id}")

    # Validate addresses before case creation
    validated_addresses = []
    basic_data = []

    dataforsyningen_url = orchestrator_connection.get_constant("DataforsyningenURL").value.rstrip("/")

    for raw_address in [address1, address2]:
        if not raw_address:
            continue

        response = requests.get(
            f"{dataforsyningen_url}/adresser",
            params={
                "kommunekode": "751",
                "struktur": "mini",
                "q": raw_address,
            },
            timeout=60,
        )
        response.raise_for_status()
        results = response.json()

        if not results:
            raise RuntimeError(f"Address could not be validated: {raw_address}")

        address_result = results[0]

        adresse_id = str(address_result["adgangsadresseid"])
        vejnavn = str(address_result["vejnavn"]).strip()
        husnr = str(address_result["husnr"]).strip()
        postnr = str(address_result["postnr"]).strip()
        postnrnavn = str(address_result["postnrnavn"]).strip()
        x = address_result["x"]
        y = address_result["y"]

        validated_address = f"{vejnavn} {husnr}, {postnr} {postnrnavn}".strip()
        validated_addresses.append(validated_address)

        # Address basic data
        basic_data.append({
            "basicDataType": 1,
            "basicDataId": adresse_id,
        })

        # Cadastral / BFE from coordinates
        response = requests.get(
            f"{dataforsyningen_url}/jordstykker/reverse",
            params={
                "x": str(x).replace(",", "."),
                "y": str(y).replace(",", "."),
            },
            timeout=60,
        )
        response.raise_for_status()
        landlot = response.json()

        if landlot:
            owner_low = landlot.get("ejerlav")
            cadastral_number = landlot.get("matrikelnr")
            bfe_number = landlot.get("bfenummer")

            if owner_low and owner_low.get("kode") and cadastral_number:
                basic_data.append({
                    "basicDataType": 3,
                    "basicDataId": f'{owner_low["kode"]} {cadastral_number}',
                })

            if bfe_number and int(bfe_number) > 0:
                basic_data.append({
                    "basicDataType": 4,
                    "basicDataId": str(bfe_number),
                })

    if not basic_data:
        raise RuntimeError("No basic data could be created from the address. Fix address manually.")

    for validated_address in validated_addresses:
        cursor.execute(
            """
            SELECT TOP 1 Id
            FROM [Xflow].[Addresses]
            WHERE ProcessId = ? AND Value = ?
            """,
            process_db_id,
            validated_address,
        )
        if cursor.fetchone() is None:
            cursor.execute(
                """
                INSERT INTO [Xflow].[Addresses] ([Value], [ProcessId])
                VALUES (?, ?)
                """,
                validated_address,
                process_db_id,
            )

    conn.commit()

    # Fetch Xflow process JSON
    xflowcredentials = orchestrator_connection.get_credential("XFlowAPI")
    xflow_api_url = xflowcredentials.username
    xflow_token = xflowcredentials.password

    response = requests.get(
        f"{xflow_api_url}/Process/{process_public_id}",
        headers={"publicApiToken": xflow_token},
        timeout=60,
    )
    response.raise_for_status()
    xflow_json = response.text

    # Store process date if missing
    if process_date is None:
        matches = re.findall(r'sidstAendretDen"\s*:\s*"([^"]+)"', xflow_json)
        raw_date = matches[0]

        try:
            dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            dt = datetime.strptime(raw_date, "%Y-%m-%dT%H:%M:%S")

        case_date = dt.date()

        cursor.execute(
            """
            UPDATE [Xflow].[Processes]
            SET [Date] = ?
            WHERE Id = ? AND [Date] IS NULL
            """,
            case_date,
            process_db_id,
        )
        conn.commit()
    else:
        case_date = process_date

    # Create FilArkiv case if missing
    if filarkiv_case_id is None:
        env_key = "Test" if use_test else "Production"

        if use_test:
            filarkiv_cred = orchestrator_connection.get_credential("FilarkivTestEnvSecret")
            filarkiv_token_url = filarkiv_cred.username
            client_secret = filarkiv_cred.password
            filarkiv_cred2 = orchestrator_connection.get_credential("FilarkivTestEnvClientId")
            env_name = filarkiv_cred2.username
            client_id = filarkiv_cred2.password
            filarkiv_url = orchestrator_connection.get_constant("FilarkivURLTest").value.rstrip("/")

        else:
            filarkiv_cred = orchestrator_connection.get_credential("FilarkivXFlowSecret")
            filarkiv_token_url = filarkiv_cred.username
            client_secret = filarkiv_cred.password
            filarkiv_cred2 = orchestrator_connection.get_credential("FilarkivXFlowClientId")
            env_name = filarkiv_cred2.username
            client_id = filarkiv_cred2.password
            filarkiv_url = orchestrator_connection.get_constant("FilarkivURL").value.rstrip("/")

        response = requests.post(
            f"{filarkiv_token_url}/fa-{env_name}/{client_id}(login)/oauth/token",
            data={
                "client_secret": client_secret,
                "client_id": client_id,
                "scope": filarkiv_info["Scope"],
                "grant_type": filarkiv_info["GrantType"],
            },
            timeout=60,
        )
        response.raise_for_status()


        access_token = response.json()["access_token"]

        archive_id = filarkiv_info[env_key]["Archives"][archive]
        case_type_id = filarkiv_info[env_key]["Sagstyper"]["Ingen"]
        case_status_id = filarkiv_info[env_key]["Sagsstatusser"]["Afsluttet"]


        case_number = f"{int(form_id)}-{case_date.year}-{case_date.month}"
        case_reference = str(payload.get("CaseReference") or f"xflow:{process_public_id}").strip()
        case_date_iso = datetime(case_date.year, case_date.month, case_date.day, 0, 0, 0).isoformat()

        orchestrator_connection.log_info(
            f"Creating FilArkiv case CaseNumber={case_number}, Reference={case_reference}"
        )

        response = requests.post(
            f"{filarkiv_url}/Cases",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json={
                "caseNumber": case_number,
                "title": case_title,
                "caseReference": case_reference,
                "caseDate": case_date_iso,
                "caseTypeId": case_type_id,
                "caseStatusId": case_status_id,
                "archiveId": archive_id,
                "securityClassificationLevel": 0,
                "basicData": basic_data,
            },
            timeout=60,
        )
        response.raise_for_status()

        filarkiv_case_id = response.json()["id"]

        cursor.execute(
            """
            UPDATE [Xflow].[Processes]
            SET FilArkivCaseId = ?
            WHERE Id = ?
            """,
            int(filarkiv_case_id),
            process_db_id,
        )
        conn.commit()
    else:
        filarkiv_case_id = int(filarkiv_case_id)

    # Extract document guids
    guids = re.findall(
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
        xflow_json,
    )

    doc_guids = []
    seen = set()
    for guid in guids:
        guid = guid.lower()
        if guid not in seen:
            seen.add(guid)
            doc_guids.append(guid)

    if not doc_guids:
        orchestrator_connection.log_info("No document GUIDs found in Xflow response.")
        return

    # Register missing Xflow documents
    for doc_guid in doc_guids:
        cursor.execute(
            """
            SELECT TOP 1 Id
            FROM [Xflow].[Documents]
            WHERE XflowId = ?
            """,
            doc_guid,
        )
        if cursor.fetchone() is None:
            cursor.execute(
                """
                INSERT INTO [Xflow].[Documents] ([AddedAt], [XflowId], [ProcessId])
                VALUES (SYSUTCDATETIME(), ?, ?)
                """,
                doc_guid,
                process_db_id,
            )

    conn.commit()

    # Enqueue missing document jobs
    enqueued = 0
    for doc_guid in doc_guids:
        cursor.execute(
            """
            SELECT TOP 1 StorageId
            FROM [Xflow].[Documents]
            WHERE XflowId = ? AND ProcessId = ?
            """,
            doc_guid,
            process_db_id,
        )
        row = cursor.fetchone()
        storage_id = row[0] if row else None

        if storage_id is not None:
            cursor.execute(
                """
                SELECT TOP 1 Id
                FROM [FilArkiv].[Documents]
                WHERE CaseId = ? AND StorageId = ?
                """,
                filarkiv_case_id,
                storage_id,
            )
            if cursor.fetchone() is not None:
                continue

        orchestrator_connection.create_queue_element(
            "XFlowToFilarkivDocuments",
            reference=f"{process_public_id}:{doc_guid}",
            data=json.dumps(
                {
                    "ProcessPublicId": process_public_id,
                    "ProcessDbId": process_db_id,
                    "XflowDocumentId": doc_guid,
                    "FilArkivCaseId": filarkiv_case_id,
                    "UseTestEnvironment": use_test,
                    "Archive": archive,
                },
                ensure_ascii=False,
            ),
        )
        enqueued += 1

    cursor.execute(
        """
        UPDATE [Xflow].[Processes]
        SET DocumentRegisteredAt = COALESCE(DocumentRegisteredAt, SYSUTCDATETIME())
        WHERE Id = ?
        """,
        process_db_id,
    )
    conn.commit()

    orchestrator_connection.log_info(
        f"Done for ProcessPublicId={process_public_id}. Enqueued {enqueued} document jobs."
    )