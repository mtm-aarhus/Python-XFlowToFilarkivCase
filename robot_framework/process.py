"""This module contains the main process of the robot."""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement

import io
import json
import os
import re
from datetime import datetime
from urllib.parse import unquote

import pyodbc
import requests
import magic


def process(
    orchestrator_connection: OrchestratorConnection,
    queue_element: QueueElement | None = None,
) -> None:
    payload = json.loads(queue_element.data)
    filarkiv_info = json.loads(
        orchestrator_connection.get_constant("XFlowFilarkivValues").value
    )

    (
        process_public_id,
        form_id,
        case_title,
        form_title,
        archive,
        address1,
        address2,
        use_test,
    ) = normalize_payload(payload)

    orchestrator_connection.log_info(
        f"Start ProcessPublicId={process_public_id}, FormId={form_id}, Archive={archive}, Test={use_test}"
    )

    conn = open_connection(orchestrator_connection)
    cursor = conn.cursor()

    xflow_api_url, xflow_token = get_xflow_access(orchestrator_connection)
    address_services = get_address_services(orchestrator_connection)

    # -------------------------------------------------------------------------
    # 1. Ensure Xflow.Processes exists and load linked FilArkiv case info if any
    # -------------------------------------------------------------------------
    process_state = get_or_create_process_row(
        cursor=cursor,
        conn=conn,
        process_public_id=process_public_id,
        form_id=form_id,
        case_title=case_title,
        archive=archive,
        form_title=form_title,
        use_test=use_test,
    )

    process_db_id = process_state["process_db_id"]
    process_date = process_state["process_date"]
    local_case_row_id = process_state["local_case_row_id"]
    external_case_uuid = process_state["external_case_uuid"]

    # -------------------------------------------------------------------------
    # 2. Store addresses on Xflow.Processes
    # -------------------------------------------------------------------------
    for raw_address in [address1, address2]:
        insert_xflow_address_if_missing(cursor, process_db_id, raw_address)
    conn.commit()

    # -------------------------------------------------------------------------
    # 3. Fetch XFlow process JSON
    # -------------------------------------------------------------------------
    xflow_json = fetch_xflow_process_json(
        xflow_api_url=xflow_api_url,
        xflow_token=xflow_token,
        process_public_id=process_public_id,
    )

    # -------------------------------------------------------------------------
    # 4. Ensure process date
    # -------------------------------------------------------------------------
    if process_date is None:
        process_date = extract_case_date(xflow_json)
        cursor.execute(
            """
            UPDATE [Xflow].[Processes]
            SET [Date] = ?
            WHERE Id = ? AND [Date] IS NULL
            """,
            process_date,
            process_db_id,
        )
        conn.commit()

    case_date = process_date

    # -------------------------------------------------------------------------
    # 5. Build basic data
    # -------------------------------------------------------------------------
    basic_data, validated_addresses = build_basic_data(
        [address1, address2],
        address_services,
    )

    for validated_address in validated_addresses:
        insert_xflow_address_if_missing(cursor, process_db_id, validated_address)
    conn.commit()

    if not basic_data:
        raise ValueError(
            f"No basic data resolvable via Adressevask/Adressevælger for XflowId={process_public_id} "
            f"(addresses tried: {[a for a in [address1, address2] if a]}). "
            "Fix the address in Xflow and retry the queue element."
        )

    # -------------------------------------------------------------------------
    # 6. Ensure FilArkiv case exists
    # -------------------------------------------------------------------------
    access_token, filarkiv_url, env_key = get_filarkiv_access(
        orchestrator_connection=orchestrator_connection,
        filarkiv_info=filarkiv_info,
        use_test=use_test,
    )

    if external_case_uuid is None or local_case_row_id is None:
        local_case_row_id, external_case_uuid = create_and_store_filarkiv_case(
            cursor=cursor,
            conn=conn,
            filarkiv_url=filarkiv_url,
            access_token=access_token,
            filarkiv_info=filarkiv_info,
            env_key=env_key,
            use_test=use_test,
            archive=archive,
            form_id=form_id,
            case_date=case_date,
            case_title=case_title,
            process_public_id=process_public_id,
            payload=payload,
            process_db_id=process_db_id,
            orchestrator_connection=orchestrator_connection,
            basic_data=basic_data,
        )

    # -------------------------------------------------------------------------
    # 6b. Sync any basic data that isn't in FilArkiv yet (replay path)
    # -------------------------------------------------------------------------
    append_missing_basic_data_to_filarkiv(
        cursor=cursor,
        conn=conn,
        filarkiv_url=filarkiv_url,
        access_token=access_token,
        external_case_uuid=external_case_uuid,
        local_case_row_id=local_case_row_id,
        basic_data=basic_data,
        orchestrator_connection=orchestrator_connection,
    )

    # -------------------------------------------------------------------------
    # 7. Build document sources
    #    Document #1 is always the form PDF
    # -------------------------------------------------------------------------
    doc_guids = extract_document_guids(xflow_json)

    form_pdf = fetch_xflow_process_pdf(
        xflow_api_url=xflow_api_url,
        xflow_token=xflow_token,
        process_public_id=process_public_id,
        fallback_filename=f"{form_title}.pdf",
    )

    form_filename = normalize_filename(form_pdf["filename"])
    _, form_file_extension = split_filename(form_filename)

    document_sources = [
        {
            "kind": "form_pdf",
            "xflow_id": process_public_id,
            "title": form_title,  # match C# logic
            "file_extension": form_file_extension or "pdf",
            "document_reference": f"xflow:{process_public_id}",
            "document_number": 1,
            "filename": form_filename or f"{form_title}.pdf",
            "content": form_pdf["content"],
            "content_type": form_pdf.get("content_type"),
        }
    ]

    for idx, doc_guid in enumerate(doc_guids, start=2):
        xflow_file = fetch_xflow_document(
            xflow_api_url=xflow_api_url,
            xflow_token=xflow_token,
            document_guid=doc_guid,
        )

        normalized_filename = normalize_filename(xflow_file["filename"])
        title, filename_ext = split_filename(normalized_filename)

        real_ext, detected_mime = detect_extension(
            xflow_file["content"],
            filename_ext
        )

        safe_filename = f"{title}.{real_ext}" if real_ext else title

        document_sources.append(
            {
                "kind": "attachment",
                "xflow_id": doc_guid,
                "title": title or doc_guid,
                "file_extension": real_ext,
                "document_reference": f"xflow:{doc_guid}",
                "document_number": idx,
                "filename": safe_filename,
                "content": xflow_file["content"],
                "content_type": detected_mime or xflow_file.get("content_type"),
            }
        )

    # -------------------------------------------------------------------------
    # 8. Ensure Xflow.Documents exists for attachments
    # -------------------------------------------------------------------------
    for doc_guid in doc_guids:
        cursor.execute(
            """
            SELECT TOP 1 Id
            FROM [Xflow].[Documents]
            WHERE XflowId = ? AND ProcessId = ?
            """,
            doc_guid,
            process_db_id,
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

    # -------------------------------------------------------------------------
    # 9. Create FilArkiv documents and upload bytes directly
    # -------------------------------------------------------------------------
    created_count = 0
    uploaded_count = 0

    for source in document_sources:
        existing_document = get_existing_filarkiv_document(
            cursor=cursor,
            case_row_id=local_case_row_id,
            document_reference=source["document_reference"],
        )

        if existing_document is None:
            external_document_uuid, external_file_uuid = create_filarkiv_document(
                filarkiv_url=filarkiv_url,
                access_token=access_token,
                external_case_uuid=external_case_uuid,
                title=unquote(source["title"]),
                document_reference=source["document_reference"],
                case_date=case_date,
                document_number=source["document_number"],
                filename_with_extension=unquote(source["filename"]),
            )

            filarkiv_document_row_id = insert_filarkiv_document_row(
                cursor=cursor,
                conn=conn,
                case_row_id=local_case_row_id,
                external_document_uuid=external_document_uuid,
                external_file_uuid=external_file_uuid,
                source=source,
                use_test=use_test,
            )
            uploaded_at = None
            created_count += 1
        else:
            filarkiv_document_row_id = existing_document["id"]
            external_file_uuid = existing_document["file_uuid"]
            uploaded_at = existing_document["uploaded_at"]

        if uploaded_at is None and external_file_uuid:
            upload_filarkiv_file_bytes(
                filarkiv_url=filarkiv_url,
                access_token=access_token,
                file_uuid=external_file_uuid,
                filename=source["filename"],
                content=source["content"],
                content_type=source.get("content_type"),
            )

            cursor.execute(
                """
                UPDATE [FilArkiv].[Documents]
                SET UploadedAt = COALESCE(UploadedAt, SYSUTCDATETIME())
                WHERE Id = ?
                """,
                filarkiv_document_row_id,
            )
            conn.commit()
            uploaded_count += 1

    # -------------------------------------------------------------------------
    # 10. Mark process as registered
    # -------------------------------------------------------------------------
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
        f"Done for ProcessPublicId={process_public_id}. Created {created_count} documents, uploaded {uploaded_count} files."
    )


# =============================================================================
# Database / state helpers
# =============================================================================

def open_connection(orchestrator_connection: OrchestratorConnection):
    sql_server = orchestrator_connection.get_constant("SqlServer").value
    conn_string = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={sql_server};"
        "DATABASE=XFlowToFilarkiv;"
        "Trusted_Connection=yes;"
    )
    conn = pyodbc.connect(conn_string)
    conn.autocommit = False
    return conn


def get_or_create_process_row(
    cursor,
    conn,
    process_public_id: str,
    form_id: int,
    case_title: str,
    archive: str,
    form_title: str,
    use_test: bool,
) -> dict:
    cursor.execute(
        """
        SELECT TOP 1
            p.Id,
            p.Date,
            p.FilArkivCaseId,
            c.CaseId
        FROM [Xflow].[Processes] p
        LEFT JOIN [FilArkiv].[Cases] c
            ON c.Id = p.FilArkivCaseId
        WHERE p.ProcessPublicId = ?
        """,
        process_public_id,
    )
    row = cursor.fetchone()

    if row is not None:
        return {
            "process_db_id": row[0],
            "process_date": row[1],
            "local_case_row_id": row[2],
            "external_case_uuid": str(row[3]) if row[3] is not None else None,
        }
    
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
    conn.commit()

    cursor.execute(
        """
        SELECT TOP 1 Id
        FROM [Xflow].[Processes]
        WHERE ProcessPublicId = ?
        ORDER BY Id DESC
        """,
        process_public_id,
    )
    process_db_id = cursor.fetchone()[0]

    return {
        "process_db_id": process_db_id,
        "process_date": None,
        "local_case_row_id": None,
        "external_case_uuid": None,
    }


def insert_xflow_address_if_missing(cursor, process_db_id: int, address: str) -> None:
    address = (address or "").strip()
    if not address:
        return

    cursor.execute(
        """
        SELECT TOP 1 Id
        FROM [Xflow].[Addresses]
        WHERE ProcessId = ? AND Value = ?
        """,
        process_db_id,
        address,
    )
    if cursor.fetchone() is None:
        cursor.execute(
            """
            INSERT INTO [Xflow].[Addresses] ([Value], [ProcessId])
            VALUES (?, ?)
            """,
            address,
            process_db_id,
        )


def get_existing_filarkiv_document(cursor, case_row_id: int, document_reference: str) -> dict | None:
    cursor.execute(
        """
        SELECT TOP 1 Id, FileId, UploadedAt
        FROM [FilArkiv].[Documents]
        WHERE CaseId = ? AND DocumentReference = ?
        """,
        case_row_id,
        document_reference,
    )
    row = cursor.fetchone()
    if row is None:
        return None

    return {
        "id": row[0],
        "file_uuid": str(row[1]) if row[1] is not None else None,
        "uploaded_at": row[2],
    }

def insert_filarkiv_document_row(
    cursor,
    conn,
    case_row_id: int,
    external_document_uuid: str,
    external_file_uuid: str,
    source: dict,
    use_test: bool,
) -> int:
    cursor.execute(
        """
        INSERT INTO [FilArkiv].[Documents]
            ([AddedAt], [CaseId], [DocumentId], [FileId], [DocumentNumber], [CreatedAt],
             [Title], [FileExtension], [DocumentReference], [UseTestEnvironment], [StorageId], [UploadedAt])
        VALUES
            (SYSUTCDATETIME(), ?, ?, ?, ?, SYSUTCDATETIME(),
             ?, ?, ?, ?, ?, NULL)
        """,
        case_row_id,
        external_document_uuid,
        external_file_uuid,
        source["document_number"],
        source["title"],
        source["file_extension"],
        source["document_reference"],
        1 if use_test else 0,
        "00000000-0000-0000-0000-000000000000",
    )
    conn.commit()

    cursor.execute(
        """
        SELECT TOP 1 Id
        FROM [FilArkiv].[Documents]
        WHERE DocumentId = ?
        ORDER BY Id DESC
        """,
        external_document_uuid,
    )
    row_id = cursor.fetchone()[0]
    return row_id


# =============================================================================
# Payload / parsing helpers
# =============================================================================

def normalize_payload(payload: dict) -> tuple[str, int, str, str, str, str, str, bool]:
    if "XflowId" in payload:
        process_public_id = str(payload["XflowId"]).strip()
        form_id = int(payload["FormId"])
        case_title = str(payload["CaseTitle"]).strip()
        form_title = str(payload["FormTitle"]).strip()
        archive = str(payload["Archive"]).strip()
        address1 = str(payload["Address"]).strip()
        address2 = str(payload.get("AdditionalAddress") or "").strip()
        test = str(payload.get("Test") or "").strip()
    else:
        process_public_id = str(payload["xflowArbejdsgangId"]).strip()
        form_id = int(payload["xflowBlanketId"])
        case_title = str(payload["beskrivelse"]).strip()
        form_title = str(payload["blankettitel"]).strip()
        archive = str(payload["arkiv"]).strip()

        adresse = str(payload.get("adresse") or "").strip()
        vejnavn = str(payload.get("vejnavn") or "").strip()
        husnummer = str(payload.get("husnummer") or "").strip()
        test = str(payload.get("Test") or "").strip()

        address1 = adresse if adresse else f"{vejnavn} {husnummer}".strip()
        address2 = ""

    use_test = test.lower() == "test"

    return process_public_id, form_id, case_title, form_title, archive, address1, address2, use_test


def extract_case_date(xflow_json: str):
    matches = re.findall(r'sidstAendretDen"\s*:\s*"([^"]+)"', xflow_json)
    raw_date = matches[0]

    try:
        dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        dt = datetime.strptime(raw_date, "%Y-%m-%dT%H:%M:%S")

    return dt.date()


def extract_document_guids(xflow_json: str) -> list[str]:
    seen = set()
    result = []

    for guid in re.findall(r'document-\d*"\s*:\s*"([^"]+)"', xflow_json, flags=re.IGNORECASE):
        guid = guid.strip().lower()
        if guid and guid not in seen:
            seen.add(guid)
            result.append(guid)

    return result


def normalize_filename(filename: str) -> str:
    filename = os.path.basename((filename or "").strip())

    title, ext = os.path.splitext(filename)
    ext = ext.lower().lstrip(".")

    if ext == "jpeg":
        ext = "jpg"

    if ext:
        return f"{title.strip()}.{ext}"
    return title.strip()


def split_filename(filename: str) -> tuple[str, str]:
    title, ext = os.path.splitext(filename)
    return title.strip(), ext.lower().lstrip(".")


def extract_filename_from_headers(headers: dict) -> str | None:
    content_disposition = (
        headers.get("Content-Disposition")
        or headers.get("content-disposition")
        or ""
    )

    # RFC 5987 / filename*=UTF-8''...
    match = re.search(r"filename\*\s*=\s*([^;]+)", content_disposition, flags=re.IGNORECASE)
    if match:
        value = match.group(1).strip().strip('"')
        if "''" in value:
            _, encoded = value.split("''", 1)
            return unquote(encoded)
        return unquote(value)

    # Plain filename="..."
    match = re.search(r'filename\s*=\s*"([^"]+)"', content_disposition, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip()

    match = re.search(r"filename\s*=\s*([^;]+)", content_disposition, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip().strip('"')

    return None

# =============================================================================
# Address / basic data helpers
# =============================================================================

# DAWA (api.dataforsyningen.dk) is closed (410 Gone). Addresses are now resolved via
# Klimadatastyrelsens Adressevask/Adressevælger, and matrikel/BFE via Datafordeleren.
# https://confluence.kds.dk/pages/viewpage.action?pageId=234782998
AARHUS_KOMMUNEKODE = "0751"
SAFE_ID_RE = re.compile(r"^[0-9A-Za-z-]+$")


def get_address_services(orchestrator_connection: OrchestratorConnection) -> dict:
    # Credentials hold URL (username) + token/API key (password).
    adressevaelger = orchestrator_connection.get_credential("Adressevælger")
    datafordeler = orchestrator_connection.get_credential("DatafordelerAPI")
    return {
        "adressevaelger_url": adressevaelger.username.rstrip("/"),
        "adressevaelger_token": adressevaelger.password,
        "datafordeler_url": datafordeler.username.rstrip("/"),
        "datafordeler_api_key": datafordeler.password,
    }


def adressevaelger_get(address_services: dict, path: str, params: dict) -> dict:
    response = requests.get(
        f"{address_services['adressevaelger_url']}{path}",
        params={**params, "token": address_services["adressevaelger_token"]},
        timeout=60,
    )
    response.raise_for_status()
    return response.json() or {}


def resolve_husnummer(raw_address: str, address_services: dict) -> dict | None:
    """Wash a free-text address into a DAR husnummer in Aarhus Kommune.

    Adressevask only answers when exactly one address matches, so fall back to the
    Adressevælger search (restricted to Aarhus) for looser input like "Vejnavn 9".
    """
    vask = adressevaelger_get(address_services, "/vask/", {"adresse": raw_address})
    adresse_id = (vask.get("vaskeresultat") or {}).get("adresse_id_lokalid")
    if adresse_id:
        adresse = adressevaelger_get(address_services, f"/adresser/{adresse_id}", {}).get("adresse") or {}
        husnummer = adresse.get("husnummer") or {}
        if (husnummer.get("navngivenvejkommunedel") or {}).get("kommune") == AARHUS_KOMMUNEKODE:
            return husnummer

    search = adressevaelger_get(
        address_services,
        "/husnumre/soeg",
        {"tekst": raw_address, "kommunekode": AARHUS_KOMMUNEKODE, "maksimum": 1},
    )
    hit = next(
        (f for f in search.get("fund") or [] if f.get("type") in ("husnummer", "adresse") and f.get("id")),
        None,
    )
    if hit is None:
        return None

    if hit["type"] == "adresse":
        adresse = adressevaelger_get(address_services, f"/adresser/{hit['id']}", {}).get("adresse") or {}
        return adresse.get("husnummer") or None
    return adressevaelger_get(address_services, f"/husnumre/{hit['id']}", {}).get("husnummer") or None


def datafordeler_query(address_services: dict, query: str) -> dict:
    response = requests.post(
        address_services["datafordeler_url"],
        params={"apiKey": address_services["datafordeler_api_key"]},
        json={"query": query},
        headers={"Content-Type": "application/json"},
        timeout=60,
    )
    if not response.ok:
        # Don't let raise_for_status() put the apiKey (in the URL) into the OO log.
        raise RuntimeError(f"Datafordeler GraphQL HTTP {response.status_code}: {response.text[:500]}")
    payload = response.json() or {}
    if payload.get("errors"):
        raise RuntimeError(f"Datafordeler GraphQL error: {payload['errors'][0].get('message')}")
    return payload.get("data") or {}


def datafordeler_nodes(address_services: dict, entity: str, local_id: str, fields: str) -> list[dict]:
    if not SAFE_ID_RE.match(local_id):
        raise ValueError(f"Unexpected {entity} id: {local_id!r}")

    ts = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    query = f"""
    query {{
      {entity}(
        registreringstid: "{ts}"
        virkningstid: "{ts}"
        first: 10
        where: {{ id_lokalId: {{ eq: "{local_id}" }} }}
      ) {{
        nodes {{ {fields} }}
      }}
    }}
    """
    return (datafordeler_query(address_services, query).get(entity) or {}).get("nodes") or []


def lookup_jordstykke(husnummer_id: str, address_services: dict) -> dict | None:
    """Replaces DAWA /jordstykker/reverse: husnummer -> jordstykke -> ejerlav + BFE.

    DAWA found the jordstykke by point-in-polygon on the adgangspunkt; DAR registers
    that same jordstykke on the husnummer, so we follow the relation instead.
    Field names follow KDS' "Udvalgte REST-tjenester omsat til GraphQL-queries".
    """
    husnumre = datafordeler_nodes(address_services, "DAR_Husnummer", husnummer_id, "id_lokalId jordstykke")
    jordstykke_id = next((str(h["jordstykke"]) for h in husnumre if h.get("jordstykke")), None)
    if not jordstykke_id:
        return None

    jordstykker = datafordeler_nodes(
        address_services,
        "MAT_Jordstykke",
        jordstykke_id,
        "id_lokalId matrikelnummer ejerlavLokalId samletFastEjendomLokalId status",
    )
    if not jordstykker:
        return None
    jordstykke = next(
        (j for j in jordstykker if str(j.get("status") or "").lower() == "gældende"),
        jordstykker[0],
    )

    ejerlavskode = None
    ejerlav_id = str(jordstykke.get("ejerlavLokalId") or "")
    if ejerlav_id:
        ejerlav = datafordeler_nodes(address_services, "MAT_Ejerlav", ejerlav_id, "id_lokalId ejerlavskode")
        ejerlavskode = next((e["ejerlavskode"] for e in ejerlav if e.get("ejerlavskode") is not None), None)

    bfenummer = None
    sfe_id = str(jordstykke.get("samletFastEjendomLokalId") or "")
    if sfe_id:
        sfe = datafordeler_nodes(address_services, "MAT_SamletFastEjendom", sfe_id, "id_lokalId BFEnummer")
        bfenummer = next((s["BFEnummer"] for s in sfe if s.get("BFEnummer") is not None), None)

    return {
        "ejerlavskode": ejerlavskode,
        "matrikelnummer": jordstykke.get("matrikelnummer"),
        "bfenummer": bfenummer,
    }


def build_basic_data(addresses: list[str], address_services: dict) -> tuple[list[dict], list[str]]:
    basic_data = []
    validated_addresses = []
    seen_basic = set()

    def add_basic_data(basic_data_type: int, basic_data_id: str) -> None:
        basic_data_id = str(basic_data_id or "").strip()
        if not basic_data_id:
            return

        key = (basic_data_type, basic_data_id)
        if key in seen_basic:
            return

        seen_basic.add(key)
        basic_data.append(
            {
                "basicDataType": basic_data_type,
                "basicDataId": basic_data_id,
            }
        )

    for raw_address in addresses:
        raw_address = (raw_address or "").strip()
        if not raw_address:
            continue

        husnummer = resolve_husnummer(raw_address, address_services)
        if not husnummer:
            continue

        # DAR husnummer id == the old DAWA adgangsadresseid, so existing basic data still matches.
        adresse_id = str(husnummer.get("id_lokalid") or "").strip()
        postnummer = husnummer.get("postnummer") or {}
        vejnavn = str(husnummer.get("vejnavn") or "").strip()
        husnr = str(husnummer.get("husnummertekst") or "").strip()
        postnr = str(postnummer.get("postnr") or "").strip()
        postnrnavn = str(postnummer.get("navn") or "").strip()

        # Same format as the DAWA era so replays don't add duplicate Xflow.Addresses rows.
        validated_address = f"{vejnavn} {husnr}, {postnr} {postnrnavn}".strip(" ,")
        if validated_address and validated_address not in validated_addresses:
            validated_addresses.append(validated_address)

        if not adresse_id:
            continue
        add_basic_data(1, adresse_id)

        landlot = lookup_jordstykke(adresse_id, address_services) or {}

        owner_code = landlot.get("ejerlavskode")
        cadastral_number = landlot.get("matrikelnummer")
        bfe_number = landlot.get("bfenummer")

        if owner_code is not None and cadastral_number:
            add_basic_data(3, f"{owner_code} {cadastral_number}")

        if bfe_number is not None:
            try:
                bfe_int = int(bfe_number)
                if bfe_int > 0:
                    add_basic_data(4, str(bfe_int))
            except (TypeError, ValueError):
                pass

    return basic_data, validated_addresses


# =============================================================================
# XFlow helpers
# =============================================================================

def get_xflow_access(orchestrator_connection: OrchestratorConnection) -> tuple[str, str]:
    xflow_credentials = orchestrator_connection.get_credential("XFlowAPI")
    return xflow_credentials.username.rstrip("/"), xflow_credentials.password


def fetch_xflow_process_json(
    xflow_api_url: str,
    xflow_token: str,
    process_public_id: str,
) -> str:
    response = requests.get(
        f"{xflow_api_url}/Process/{process_public_id}",
        headers={"publicApiToken": xflow_token},
        timeout=60,
    )
    response.raise_for_status()
    return response.text


def fetch_xflow_process_pdf(
    xflow_api_url: str,
    xflow_token: str,
    process_public_id: str,
    fallback_filename: str,
) -> dict:
    response = requests.get(
        f"{xflow_api_url}/Process/{process_public_id}/pdf",
        headers={"publicApiToken": xflow_token},
        timeout=120,
    )
    response.raise_for_status()

    filename = extract_filename_from_headers(response.headers) or fallback_filename

    return {
        "filename": filename,
        "content": response.content,
        "content_type": response.headers.get("Content-Type"),
    }


def fetch_xflow_document(
    xflow_api_url: str,
    xflow_token: str,
    document_guid: str,
) -> dict:
    response = requests.get(
        f"{xflow_api_url}/Document/{document_guid}",
        headers={"publicApiToken": xflow_token},
        timeout=120,
    )
    response.raise_for_status()

    filename = extract_filename_from_headers(response.headers) or document_guid

    return {
        "filename": filename,
        "content": response.content,
        "content_type": response.headers.get("Content-Type"),
    }


# =============================================================================
# FilArkiv helpers
# =============================================================================

def generate_case_number(case_date, form_id: int) -> str:
    return f"{int(form_id)}-{case_date.year}-{case_date.month}"


def get_filarkiv_access(
    orchestrator_connection: OrchestratorConnection,
    filarkiv_info: dict,
    use_test: bool,
) -> tuple[str, str, str]:
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

    token_response = requests.post(
        f"{filarkiv_token_url}/fa-{env_name}/{client_id}(login)/oauth/token",
        data={
            "client_secret": client_secret,
            "client_id": client_id,
            "scope": filarkiv_info["Scope"],
            "grant_type": filarkiv_info["GrantType"],
        },
        timeout=60,
    )
    token_response.raise_for_status()

    return token_response.json()["access_token"], filarkiv_url, env_key


def create_and_store_filarkiv_case(
    cursor,
    conn,
    filarkiv_url: str,
    access_token: str,
    filarkiv_info: dict,
    env_key: str,
    use_test: bool,
    archive: str,
    form_id: int,
    case_date,
    case_title: str,
    process_public_id: str,
    payload: dict,
    process_db_id: int,
    orchestrator_connection: OrchestratorConnection,
    basic_data: list[dict],
) -> tuple[int, str]:
    archive_id = filarkiv_info[env_key]["Archives"][archive]
    case_type_id = filarkiv_info[env_key]["Sagstyper"]["Ingen"]
    case_status_id = filarkiv_info[env_key]["Sagsstatusser"]["Afsluttet"]

    case_number = generate_case_number(case_date, form_id)
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

    external_case_uuid = str(response.json()["id"])

    archive_lookup = {
        "Olietank": 0,
        "Kloak": 1,
        "Byggesag": 2,
        "BBR": 3,
    }

    cursor.execute(
        """
        INSERT INTO [FilArkiv].[Cases]
            ([AddedAt], [CreatedAt], [CaseId], [CaseNumber], [CaseTitle], [CaseDate], [CaseReference], [UseTestEnvironment], [Archive])
        VALUES
            (SYSUTCDATETIME(), SYSUTCDATETIME(), ?, ?, ?, ?, ?, ?, ?)
        """,
        external_case_uuid,
        case_number,
        case_title,
        case_date.strftime("%Y-%m-%d"),
        case_reference,
        1 if use_test else 0,
        archive_lookup[archive],
    )
    conn.commit()

    cursor.execute(
        """
        SELECT TOP 1 Id
        FROM [FilArkiv].[Cases]
        WHERE CaseId = ?
        ORDER BY Id DESC
        """,
        external_case_uuid,
    )
    local_case_row_id = cursor.fetchone()[0]

    cursor.execute(
        """
        UPDATE [Xflow].[Processes]
        SET FilArkivCaseId = ?
        WHERE Id = ?
        """,
        local_case_row_id,
        process_db_id,
    )

    for entry in basic_data:
        insert_basic_data_sync_row(
            cursor=cursor,
            case_row_id=local_case_row_id,
            basic_data_type=entry["basicDataType"],
            basic_data_id=entry["basicDataId"],
        )

    conn.commit()

    orchestrator_connection.log_info(
        f"Created FilArkiv case. External uuid={external_case_uuid}, local row id={local_case_row_id}"
    )

    return local_case_row_id, external_case_uuid


def insert_basic_data_sync_row(
    cursor,
    case_row_id: int,
    basic_data_type: int,
    basic_data_id: str,
) -> None:
    cursor.execute(
        """
        INSERT INTO [FilArkiv].[BasicData]
            ([AddedAt], [CaseId], [BasicDataType], [BasicDataId])
        VALUES
            (SYSUTCDATETIME(), ?, ?, ?)
        """,
        case_row_id,
        basic_data_type,
        basic_data_id,
    )


def append_missing_basic_data_to_filarkiv(
    cursor,
    conn,
    filarkiv_url: str,
    access_token: str,
    external_case_uuid: str,
    local_case_row_id: int,
    basic_data: list[dict],
    orchestrator_connection: OrchestratorConnection,
) -> None:
    added = 0

    for entry in basic_data:
        cursor.execute(
            """
            SELECT TOP 1 Id
            FROM [FilArkiv].[BasicData]
            WHERE CaseId = ?
              AND BasicDataType = ?
              AND BasicDataId = ?
            """,
            local_case_row_id,
            entry["basicDataType"],
            entry["basicDataId"],
        )
        if cursor.fetchone() is not None:
            continue

        response = requests.post(
            f"{filarkiv_url}/BasicData",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            json={
                "caseId": external_case_uuid,
                "basicDataType": entry["basicDataType"],
                "basicDataId": entry["basicDataId"],
            },
            timeout=60,
        )
        response.raise_for_status()

        insert_basic_data_sync_row(
            cursor=cursor,
            case_row_id=local_case_row_id,
            basic_data_type=entry["basicDataType"],
            basic_data_id=entry["basicDataId"],
        )
        added += 1

    if added:
        conn.commit()
        orchestrator_connection.log_info(
            f"Appended {added} basic data entries to FilArkiv case {external_case_uuid}"
        )

def create_filarkiv_document(
    filarkiv_url: str,
    access_token: str,
    external_case_uuid: str,
    title: str,
    document_reference: str,
    case_date,
    document_number: int,
    filename_with_extension: str,
) -> tuple[str, str | None]:
    response = requests.post(
        f"{filarkiv_url}/Documents",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        json={
            "caseId": external_case_uuid,
            "documentReference": document_reference,
            "title": title,
            "documentNumber": document_number,
            "documentDate": datetime(
                case_date.year, case_date.month, case_date.day, 0, 0, 0
            ).isoformat(),
            "files": [
                {
                    "fileName": filename_with_extension,
                    "reference": document_reference,
                }
            ],
        },
        timeout=60,
    )
    response.raise_for_status()

    payload = response.json()
    file_id = None

    files = payload.get("files") or []
    if files:
        first_file = files[0] or {}
        file_id = first_file.get("id")

    return str(payload["id"]), (str(file_id) if file_id is not None else None)

def upload_filarkiv_file_bytes(
    filarkiv_url: str,
    access_token: str,
    file_uuid: str,
    filename: str,
    content: bytes,
    content_type: str | None,
) -> None:
    files = {
        "file": (
            filename,
            io.BytesIO(content),
            content_type or "application/octet-stream",
        )
    }

    response = requests.post(
        f"{filarkiv_url}/fileio/upload/{file_uuid}",
        headers={
            "Authorization": f"Bearer {access_token}",
        },
        files=files,
        timeout=300,
    )
    response.raise_for_status()



def detect_extension(content: bytes, fallback_ext: str = "") -> tuple[str, str]:
    fallback_ext = (fallback_ext or "").lower().lstrip(".")

    try:
        mime = magic.from_buffer(content, mime=True)
    except Exception:
        mime = None

    mime_to_ext = {
        "application/pdf": "pdf",
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/gif": "gif",
        "image/tiff": "tif",
        "image/heic": "heic",
        "image/heif": "heif",
        "image/avif": "avif",
    }

    ext = mime_to_ext.get(mime, fallback_ext)

    # normalize jpeg → jpg like C#
    if ext == "jpeg":
        ext = "jpg"

    return ext, mime