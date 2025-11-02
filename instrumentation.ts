
/**
 * Este arquivo é executado automaticamente pelo Next.js quando o servidor inicia
 * Ele roda apenas UMA VEZ, antes de qualquer requisição
 * 
 * Documentação: https://nextjs.org/docs/app/building-your-application/optimizing/instrumentation
 */

export async function register() {
  console.log('🔧 [INSTRUMENTATION] Função register() foi chamada');
  console.log('🔧 [INSTRUMENTATION] NEXT_RUNTIME:', process.env.NEXT_RUNTIME);
  
  if (process.env.NEXT_RUNTIME === 'nodejs') {
    console.log('🚀 [INSTRUMENTATION] Inicializando servidor Next.js...');
    
    try {
      // Importar dinamicamente para evitar problemas de bundling
      const { initSankhyaToken } = await import('./lib/init-sankhya-token');
      
      console.log('📦 [INSTRUMENTATION] Módulo init-sankhya-token carregado');
      
      // Executar inicialização do token
      await initSankhyaToken();
      console.log('✅ [INSTRUMENTATION] Servidor inicializado com sucesso!');
    } catch (error) {
      console.error('❌ [INSTRUMENTATION] Erro na inicialização do servidor:', error);
    }
  } else {
    console.log('⚠️ [INSTRUMENTATION] Não está rodando no runtime nodejs, pulando inicialização');
  }
}
