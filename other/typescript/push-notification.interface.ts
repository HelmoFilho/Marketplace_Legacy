import { DefaultResponse, Informacoes } from './generica.interface';

export interface PushNotification {
  message?: Message;
}

export interface Message {
  token: string;
  notification: Notification;
  data: Data;
}

export interface Notification {
  title: string;
  body: string;
  image: string;
}

export interface Data {
  id_notificacao: string;
  rota: string;
  id_distribuidor: string;
  tipo?: string;
  grupo?: string;
  subgrupo?: string;
  id_produtos?: string;
  id_oferta?: string;
  id_cupom?: string;
  id_blog?: string;
}

export interface NotificacaoRequest {
  id_usuario: number;
}

export interface NotificacaoResponse extends DefaultResponse {
  informacoes?: Informacoes;
  dados?: Notificacao[];
}

export interface Notificacao {
  tipo: TipoNotificacao;
  conteudo: Data;
  notification: Notification;
  data: string;
  lida: boolean;
}

export enum TipoNotificacao {
  CUPOM = 'Cupom',
  PRODUTO = 'Produto',
  OFERTA = 'Oferta',
  CARRINHO = 'Carrinho',
  NOTICIA = 'Noticia',
}

[
  {
    tipo: TipoNotificacao.CUPOM,
    conteudo: {
      id_notificacao: '0001',
      rota: 'cupom',
      id_distribuidor: '1',
      id_cupom: '1',
    },
    notification: {
      title: 'Cupom de desconto 10OFF',
      body: 'Voce ganhou 10OFF, eita',
      image: 'https://www.pontofrio-imagens.com.br/criacao/03-hotsite/2020/03-marco/30-hrn/hotsite-cupom-2020_01.png',
    },
    data: '2022-07-14 10:12:51.913',
    lida: false,
  },

  {
    tipo: TipoNotificacao.CUPOM,
    conteudo: {
      id_notificacao: '0002',
      rota: 'cupom',
      id_distribuidor: '1',
      id_cupom: '2',
    },
    notification: {
      title: 'Cupom de desconto 20OFF',
      body: 'Voce ganhou 20OFF. APROVEITE mi amigo!!',
      image: 'https://www.smplaces.com.br/blog/wp-content/uploads/2019/08/cupom.png',
    },
    data: '2022-07-14 10:12:51.913',
    lida: false,
  },
  {
    tipo: TipoNotificacao.OFERTA,
    conteudo: {
      id_notificacao: '0003',
      rota: 'offers',
      id_distribuidor: '1',
      id_oferta: '1',
    },
    notification: {
      title: 'Ofertas produto Elseve',
      body: 'Toda a linha elseve com oferta',
      image:
        'https://a-static.mlcdn.com.br/1500x1500/kit-shampoo-e-condicionador-desmaia-fios-elseve-liso-dos-sonhos-loreal-paris-200ml-queratina-vegetal-manteiga-de-cacau-l-oreal-paris/hmzxonline2/12480563678/defaf452cf764c856e5832f6d0ba1da2.jpg',
    },
    data: '2022-07-14 10:12:51.913',
    lida: false,
  },
  {
    tipo: TipoNotificacao.OFERTA,
    conteudo: {
      id_notificacao: '0004',
      rota: 'offers',
      id_distribuidor: '1',
      id_oferta: '2',
    },
    notification: {
      title: 'Pagamento realizado com sucesso',
      body: 'O valor reservado em seu cartão de crédito, referente ao Pedido 9DSHBH, foi atualizado. Valor original do pedido: R$ 773.57. Pagamento realizado no valor de R$ 354.32',
      image: 'https://lojasrede.vteximg.com.br/arquivos/ids/320184-800-800/738616-1.png?v=637738767707000000',
    },
    data: '2022-07-14 10:12:51.913',
    lida: false,
  },
  {
    tipo: TipoNotificacao.PRODUTO,
    conteudo: {
      id_notificacao: '0005',
      rota: 'products',
      id_distribuidor: '1',
      tipo: '1',
      grupo: '13',
      subgrupo: '68',
    },
    notification: {
      title: 'PAIS CHEIROSOS',
      body: 'Polvilho Antisséptico GRANADO: um ícone da marca e um produto que faz parte da rotina dos PAIS CHEIROSOS',
      image: 'https://cdn.pixabay.com/photo/2013/11/24/11/10/lab-217043_960_720.jpg',
    },
    data: '2022-07-14 10:12:51.913',
    lida: false,
  },
];