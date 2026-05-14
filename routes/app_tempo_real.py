from flask import Blueprint, render_template, abort

app_tempo_real_bp = Blueprint(
    "app_tempo_real",
    __name__,
)

@app_tempo_real_bp.route("/app/jogo/<int:partida_id>/<perfil>")
def app_jogo_tempo_real(partida_id, perfil):

    perfis_validos = [
        "arbitro1",
        "arbitro2",
        "treinador",
        "telao",
    ]

    if perfil not in perfis_validos:
        abort(404)

    return render_template(
        "app_tempo_real.html",
        partida_id=partida_id,
        perfil=perfil,
    )