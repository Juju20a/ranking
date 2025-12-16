CREATE TABLE IF NOT EXISTS tb_instituicao (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT NOT NULL,
        nome TEXT NOT NULL,
        co_uf INTEGER NOT NULL,
        no_uf TEXT,
        sg_uf TEXT,
        co_municipio INTEGER NOT NULL,
        no_municipio TEXT,
        qt_mat_bas INTEGER NOT NULL DEFAULT 0,
        qt_mat_prof INTEGER NOT NULL DEFAULT 0,
        qt_mat_esp INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS tb_usuario (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        cpf TEXT NOT NULL UNIQUE,
        nascimento DATE NOT NULL
);

-- Tabela para ranking por ano (2022-2024) - Todo o Brasil
CREATE TABLE IF NOT EXISTS tb_instituicao_year (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        co_entidade TEXT NOT NULL,
        no_entidade TEXT,
        co_uf INTEGER,
        no_uf TEXT,
        sg_uf TEXT,
        co_municipio INTEGER,
        no_municipio TEXT,
        co_mesorregiao INTEGER,
        no_mesorregiao TEXT,
        co_microrregiao INTEGER,
        no_microrregiao TEXT,
        co_regiao INTEGER,
        no_regiao TEXT,
        nu_ano_censo INTEGER NOT NULL,
        qt_mat_bas INTEGER DEFAULT 0,
        qt_mat_prof INTEGER DEFAULT 0,
        qt_mat_eja INTEGER DEFAULT 0,
        qt_mat_esp INTEGER DEFAULT 0,
        qt_mat_fund INTEGER DEFAULT 0,
        qt_mat_inf INTEGER DEFAULT 0,
        qt_mat_med INTEGER DEFAULT 0,
        qt_mat_zr_na INTEGER DEFAULT 0,
        qt_mat_zr_rur INTEGER DEFAULT 0,
        qt_mat_zr_urb INTEGER DEFAULT 0,
        qt_mat_total INTEGER DEFAULT 0,
        UNIQUE(co_entidade, nu_ano_censo)
);

-- Adicionando restrição de unicidade para evitar duplicação por código
CREATE UNIQUE INDEX IF NOT EXISTS idx_tb_instituicao_codigo ON tb_instituicao(codigo);

-- Índices para melhorar performance do ranking
CREATE INDEX IF NOT EXISTS idx_instituicao_year_ano ON tb_instituicao_year(nu_ano_censo);
CREATE INDEX IF NOT EXISTS idx_instituicao_year_total ON tb_instituicao_year(qt_mat_total DESC);
