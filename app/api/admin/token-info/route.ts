import { NextResponse } from 'next/server';
import { obterTokenAtual } from '@/lib/sankhya-api';

export async function GET() {
  try {
    console.log('📋 [API /admin/token-info] Buscando informações do token...', {
      timestamp: new Date().toISOString()
    });

    const tokenStatus = await obterTokenAtual();

    console.log('🔍 [API /admin/token-info] Resultado do obterTokenAtual:', {
      hasTokenStatus: !!tokenStatus,
      tokenStatus: tokenStatus ? {
        ativo: tokenStatus.ativo,
        hasToken: !!tokenStatus.token,
        tokenPreview: tokenStatus.token ? tokenStatus.token.substring(0, 30) + '...' : null,
        tempoRestanteMs: tokenStatus.tempoRestanteMs,
        tempoRestanteMin: tokenStatus.tempoRestanteMin
      } : null
    });

    if (!tokenStatus || !tokenStatus.ativo || !tokenStatus.token) {
      console.log('⚠️ [API /admin/token-info] Token não disponível', {
        hasTokenStatus: !!tokenStatus,
        ativo: tokenStatus?.ativo,
        hasToken: !!tokenStatus?.token
      });
      return NextResponse.json({
        token: null,
        ativo: false,
        remainingTime: 0,
        createdAt: null,
        expiresIn: 0,
        mensagem: 'Nenhum token disponível. O token será gerado na próxima requisição.'
      }, {
        headers: {
          'Cache-Control': 'no-store, no-cache, must-revalidate, proxy-revalidate',
          'Pragma': 'no-cache',
          'Expires': '0'
        }
      });
    }

    const response = {
      token: tokenStatus.token,
      createdAt: tokenStatus.geradoEm,
      expiresIn: Math.floor(tokenStatus.tempoRestanteMs / 1000), // Converter para segundos
      remainingTime: Math.floor(tokenStatus.tempoRestanteMs / 1000), // Tempo restante em segundos
      ativo: tokenStatus.ativo
    };

    console.log('✅ [API /admin/token-info] Token encontrado e retornando:', {
      ativo: response.ativo,
      remainingTime: response.remainingTime,
      createdAt: tokenStatus.geradoEm,
      tokenPreview: tokenStatus.token.substring(0, 50) + '...'
    });

    return NextResponse.json(response, {
      headers: {
        'Cache-Control': 'no-store, no-cache, must-revalidate, proxy-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0'
      }
    });
  } catch (error) {
    console.error('❌ [API /admin/token-info] Erro ao obter informações do token:', error);
    return NextResponse.json(
      { 
        error: 'Erro ao obter informações do token',
        token: null,
        ativo: false,
        remainingTime: 0,
        createdAt: null,
        expiresIn: 0
      },
      { 
        status: 500,
        headers: {
          'Cache-Control': 'no-store'
        }
      }
    );
  }
}

export const dynamic = 'force-dynamic';