from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OK: _ClassVar[Status]
    DIVERGENTE: _ClassVar[Status]
    NAO_LIDER: _ClassVar[Status]
    SEM_QUORUM: _ClassVar[Status]
    PENDENTE: _ClassVar[Status]
OK: Status
DIVERGENTE: Status
NAO_LIDER: Status
SEM_QUORUM: Status
PENDENTE: Status

class Entrada(_message.Message):
    __slots__ = ("indice", "termo", "chave", "valor")
    INDICE_FIELD_NUMBER: _ClassVar[int]
    TERMO_FIELD_NUMBER: _ClassVar[int]
    CHAVE_FIELD_NUMBER: _ClassVar[int]
    VALOR_FIELD_NUMBER: _ClassVar[int]
    indice: int
    termo: int
    chave: str
    valor: str
    def __init__(self, indice: _Optional[int] = ..., termo: _Optional[int] = ..., chave: _Optional[str] = ..., valor: _Optional[str] = ...) -> None: ...

class VoteRequest(_message.Message):
    __slots__ = ("termo", "candidato_id", "ultimo_indice", "ultimo_termo")
    TERMO_FIELD_NUMBER: _ClassVar[int]
    CANDIDATO_ID_FIELD_NUMBER: _ClassVar[int]
    ULTIMO_INDICE_FIELD_NUMBER: _ClassVar[int]
    ULTIMO_TERMO_FIELD_NUMBER: _ClassVar[int]
    termo: int
    candidato_id: int
    ultimo_indice: int
    ultimo_termo: int
    def __init__(self, termo: _Optional[int] = ..., candidato_id: _Optional[int] = ..., ultimo_indice: _Optional[int] = ..., ultimo_termo: _Optional[int] = ...) -> None: ...

class VoteResponse(_message.Message):
    __slots__ = ("termo", "voto_concedido")
    TERMO_FIELD_NUMBER: _ClassVar[int]
    VOTO_CONCEDIDO_FIELD_NUMBER: _ClassVar[int]
    termo: int
    voto_concedido: bool
    def __init__(self, termo: _Optional[int] = ..., voto_concedido: _Optional[bool] = ...) -> None: ...

class FetchRequest(_message.Message):
    __slots__ = ("termo", "replica_id", "ultimo_indice", "ultimo_termo")
    TERMO_FIELD_NUMBER: _ClassVar[int]
    REPLICA_ID_FIELD_NUMBER: _ClassVar[int]
    ULTIMO_INDICE_FIELD_NUMBER: _ClassVar[int]
    ULTIMO_TERMO_FIELD_NUMBER: _ClassVar[int]
    termo: int
    replica_id: int
    ultimo_indice: int
    ultimo_termo: int
    def __init__(self, termo: _Optional[int] = ..., replica_id: _Optional[int] = ..., ultimo_indice: _Optional[int] = ..., ultimo_termo: _Optional[int] = ...) -> None: ...

class FetchResponse(_message.Message):
    __slots__ = ("termo", "status", "lider_id", "entradas", "commit_index", "indice_consistente")
    TERMO_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    LIDER_ID_FIELD_NUMBER: _ClassVar[int]
    ENTRADAS_FIELD_NUMBER: _ClassVar[int]
    COMMIT_INDEX_FIELD_NUMBER: _ClassVar[int]
    INDICE_CONSISTENTE_FIELD_NUMBER: _ClassVar[int]
    termo: int
    status: Status
    lider_id: int
    entradas: _containers.RepeatedCompositeFieldContainer[Entrada]
    commit_index: int
    indice_consistente: int
    def __init__(self, termo: _Optional[int] = ..., status: _Optional[_Union[Status, str]] = ..., lider_id: _Optional[int] = ..., entradas: _Optional[_Iterable[_Union[Entrada, _Mapping]]] = ..., commit_index: _Optional[int] = ..., indice_consistente: _Optional[int] = ...) -> None: ...

class GravarRequest(_message.Message):
    __slots__ = ("chave", "valor", "simular_queda")
    CHAVE_FIELD_NUMBER: _ClassVar[int]
    VALOR_FIELD_NUMBER: _ClassVar[int]
    SIMULAR_QUEDA_FIELD_NUMBER: _ClassVar[int]
    chave: str
    valor: str
    simular_queda: bool
    def __init__(self, chave: _Optional[str] = ..., valor: _Optional[str] = ..., simular_queda: _Optional[bool] = ...) -> None: ...

class GravarResponse(_message.Message):
    __slots__ = ("status", "lider_id", "indice", "termo", "mensagem")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    LIDER_ID_FIELD_NUMBER: _ClassVar[int]
    INDICE_FIELD_NUMBER: _ClassVar[int]
    TERMO_FIELD_NUMBER: _ClassVar[int]
    MENSAGEM_FIELD_NUMBER: _ClassVar[int]
    status: Status
    lider_id: int
    indice: int
    termo: int
    mensagem: str
    def __init__(self, status: _Optional[_Union[Status, str]] = ..., lider_id: _Optional[int] = ..., indice: _Optional[int] = ..., termo: _Optional[int] = ..., mensagem: _Optional[str] = ...) -> None: ...

class ConsultarRequest(_message.Message):
    __slots__ = ("chave",)
    CHAVE_FIELD_NUMBER: _ClassVar[int]
    chave: str
    def __init__(self, chave: _Optional[str] = ...) -> None: ...

class Registro(_message.Message):
    __slots__ = ("chave", "valor", "indice")
    CHAVE_FIELD_NUMBER: _ClassVar[int]
    VALOR_FIELD_NUMBER: _ClassVar[int]
    INDICE_FIELD_NUMBER: _ClassVar[int]
    chave: str
    valor: str
    indice: int
    def __init__(self, chave: _Optional[str] = ..., valor: _Optional[str] = ..., indice: _Optional[int] = ...) -> None: ...

class ConsultarResponse(_message.Message):
    __slots__ = ("registros", "estado")
    REGISTROS_FIELD_NUMBER: _ClassVar[int]
    ESTADO_FIELD_NUMBER: _ClassVar[int]
    registros: _containers.RepeatedCompositeFieldContainer[Registro]
    estado: EstadoNo
    def __init__(self, registros: _Optional[_Iterable[_Union[Registro, _Mapping]]] = ..., estado: _Optional[_Union[EstadoNo, _Mapping]] = ...) -> None: ...

class EstadoNo(_message.Message):
    __slots__ = ("no_id", "papel", "termo", "voto", "lider_id", "commit_index", "ultimo_indice", "log", "replicas_ativas", "commit_segurado")
    NO_ID_FIELD_NUMBER: _ClassVar[int]
    PAPEL_FIELD_NUMBER: _ClassVar[int]
    TERMO_FIELD_NUMBER: _ClassVar[int]
    VOTO_FIELD_NUMBER: _ClassVar[int]
    LIDER_ID_FIELD_NUMBER: _ClassVar[int]
    COMMIT_INDEX_FIELD_NUMBER: _ClassVar[int]
    ULTIMO_INDICE_FIELD_NUMBER: _ClassVar[int]
    LOG_FIELD_NUMBER: _ClassVar[int]
    REPLICAS_ATIVAS_FIELD_NUMBER: _ClassVar[int]
    COMMIT_SEGURADO_FIELD_NUMBER: _ClassVar[int]
    no_id: int
    papel: str
    termo: int
    voto: int
    lider_id: int
    commit_index: int
    ultimo_indice: int
    log: _containers.RepeatedCompositeFieldContainer[Entrada]
    replicas_ativas: _containers.RepeatedScalarFieldContainer[int]
    commit_segurado: bool
    def __init__(self, no_id: _Optional[int] = ..., papel: _Optional[str] = ..., termo: _Optional[int] = ..., voto: _Optional[int] = ..., lider_id: _Optional[int] = ..., commit_index: _Optional[int] = ..., ultimo_indice: _Optional[int] = ..., log: _Optional[_Iterable[_Union[Entrada, _Mapping]]] = ..., replicas_ativas: _Optional[_Iterable[int]] = ..., commit_segurado: _Optional[bool] = ...) -> None: ...

class DepurarRequest(_message.Message):
    __slots__ = ("acao",)
    ACAO_FIELD_NUMBER: _ClassVar[int]
    acao: str
    def __init__(self, acao: _Optional[str] = ...) -> None: ...

class Vazio(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
